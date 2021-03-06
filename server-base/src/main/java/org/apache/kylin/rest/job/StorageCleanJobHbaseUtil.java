/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageCleanJobHbaseUtil {

    protected static final Logger logger = LoggerFactory.getLogger(StorageCleanJobHbaseUtil.class);

    @SuppressWarnings("deprecation")
    public static List<String> cleanUnusedHBaseTables(boolean delete, int deleteTimeout, int threadsNum)
            throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        try (HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf)) {
            return cleanUnusedHBaseTables(hbaseAdmin, delete, deleteTimeout, threadsNum);
        }

    }

    static List<String> cleanUnusedHBaseTables(HBaseAdmin hbaseAdmin, boolean delete, int deleteTimeout, int threadsNum)
            throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(config);

        // get all kylin hbase tables
        String namespace = config.getHBaseStorageNameSpace();
        String tableNamePrefix = (namespace.equals("default") || namespace.equals(""))
                ? config.getHBaseTableNamePrefix()
                : (namespace + ":" + config.getHBaseTableNamePrefix());

        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");

        List<String> allTablesNeedToBeDropped = new ArrayList<String>();

        boolean hasExtLookupTable = false;
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(IRealizationConstants.HTableTag);
            if (config.getMetadataUrlPrefix().equalsIgnoreCase(host)) {
                // check if there are hbase lookup table
                if (desc.getTableName().getNameAsString()
                        .contains(IRealizationConstants.LookupHbaseStorageLocationPrefix)) {
                    hasExtLookupTable = true;
                }
                //only take care htables that belongs to self, and created more than 2 days
                allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
            }
        }

        // remove every segment htable from drop list
        for (CubeInstance cube : cubeMgr.reloadAndListAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String tablename = seg.getStorageLocationIdentifier();
                if (allTablesNeedToBeDropped.contains(tablename)) {
                    allTablesNeedToBeDropped.remove(tablename);
                    logger.info("Exclude table " + tablename + " from drop list, as the table belongs to cube "
                            + cube.getName() + " with status " + cube.getStatus());
                }
            }
        }

        if (allTablesNeedToBeDropped.isEmpty()) {
            logger.info("No HTable to clean up");
            return allTablesNeedToBeDropped;
        }

        logger.info(allTablesNeedToBeDropped.size() + " HTable(s) to clean up");

        if (hasExtLookupTable) {
            List<String> useExtLookupTables = getAllUsedExtLookupTables();
            logger.info("Exclude tables:{}, as they are referred by snapshots.", useExtLookupTables);
            allTablesNeedToBeDropped.removeAll(useExtLookupTables);
        }
        if (delete) {
            // drop tables
            logger.info("Use {} threads to drop unused hbase tables", threadsNum);
            ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
            List<Future<String>> tasks = Lists.newArrayListWithExpectedSize(allTablesNeedToBeDropped.size());
            for (String htableName : allTablesNeedToBeDropped) {
                tasks.add(executorService.submit(new DeleteHTableRunnable(hbaseAdmin, htableName)));
            }

            String htableName = "";
            for (Future<String> futureTask : tasks) {
                if (!futureTask.isDone()) {
                    try {
                        htableName = futureTask.get(deleteTimeout, TimeUnit.MINUTES);
                    } catch (TimeoutException e) {
                        logger.error("It fails to delete HTable " + htableName + ", for it cost more than "
                                + deleteTimeout + " minutes.", e);
                        futureTask.cancel(true);
                    } catch (Exception e) {
                        logger.error("Failed to delete HTable " + htableName, e);
                        futureTask.cancel(true);
                    }
                }
            }
            executorService.shutdown();
        } else {
            for (String htableName : allTablesNeedToBeDropped) {
                logger.info("Dry run, pending delete HTable " + htableName);
            }
        }

        return allTablesNeedToBeDropped;
    }

    private static List<String> getAllUsedExtLookupTables() throws IOException {
        List<String> result = Lists.newArrayList();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final Set<String> activeSnapshotSet = ExtTableSnapshotInfoManager.getInstance(config)
                .getAllExtSnapshotResPaths();

        for (String extSnapshotResource : activeSnapshotSet) {
            try {
                ExtTableSnapshotInfo extTableSnapshot = ExtTableSnapshotInfoManager.getInstance(config)
                        .getSnapshot(extSnapshotResource);
                if (extTableSnapshot != null) {
                    if (ExtTableSnapshotInfo.STORAGE_TYPE_HBASE.equals(extTableSnapshot.getStorageType())) {
                        result.add(extTableSnapshot.getStorageLocationIdentifier());
                    }
                }
            } catch (Exception e) {
                logger.error("error fetch ext table snapshot:" + extSnapshotResource, e);
            }
        }
        return result;
    }

    static class DeleteHTableRunnable implements Callable<String> {
        HBaseAdmin hbaseAdmin;
        String htableName;

        DeleteHTableRunnable(HBaseAdmin hbaseAdmin, String htableName) {
            this.hbaseAdmin = hbaseAdmin;
            this.htableName = htableName;
        }

        public String call() throws Exception {
            if (hbaseAdmin.tableExists(htableName)) {
                if (hbaseAdmin.isTableEnabled(htableName)) {
                    hbaseAdmin.disableTable(htableName);
                }

                hbaseAdmin.deleteTable(htableName);
            } else {
                logger.info("HBase table " + htableName + " does not exist");
            }
            return htableName;
        }
    }

}
