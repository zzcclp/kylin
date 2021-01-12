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
package org.apache.kylin.query.runtime.plans

import org.apache.calcite.DataContext
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.kylin.query.relnode.OLAPProjectRel
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.KylinFunctions._
import scala.collection.JavaConverters._

object ProjectPlan extends Logging {
  def select(
    inputs: java.util.List[DataFrame],
    rel: OLAPProjectRel,
    dataContext: DataContext): DataFrame = {

    val start = System.currentTimeMillis()
    val df = inputs.get(0)
    val duplicatedColumnsCount = collection.mutable.Map[Column, Int]()
    val olapContext = rel.getContext
    val selectedColumnsTuples = rel.rewriteProjects.asScala.zipWithIndex.toArray
      .map(rexTuple => {
        var rex = rexTuple._1
        val idx = rexTuple._2
        val visitor = new SparderRexVisitor(df,
          rel.getInput.getRowType,
          dataContext,
          if (olapContext.expsColsReplaceCols.containsKey(idx))
            olapContext.expsColsReplaceCols.get(idx) else null,
          olapContext.expsNeedReplaceCols.containsKey(idx))
        if (olapContext.expsIsRexCall.containsKey(idx)) {
          rex = rel.getOriginExps.get(idx).asInstanceOf[RexCall]
          (rex.accept(visitor), rex.isInstanceOf[RexInputRef])
        } else {
          (rex.accept(visitor), rex.isInstanceOf[RexInputRef])
        }
      })
    val selectedColumns = selectedColumnsTuples.zipWithIndex
      .map(c => {
        //  add p0,p1 suffix for window queries will generate
        // indicator columns like false,false,false
        if (c._1._2) {
          k_lit(c._1._1)
        } else {
          k_lit(c._1._1).as(s"${System.identityHashCode(rel)}_prj${c._2}")
        }
      })
      .map(c => { // find and rename the duplicated columns
        if (!(duplicatedColumnsCount contains c)) {
          duplicatedColumnsCount += (c -> 0)
          c
        } else {
          val columnCnt = duplicatedColumnsCount(c) + 1
          duplicatedColumnsCount += (c -> columnCnt)
          c.as(s"${c.toString}_duplicated$columnCnt")
        }
      })

    val prj = df.select(selectedColumns: _*)
    logTrace(s"Gen project cost Time :${System.currentTimeMillis() - start} ")
    prj
  }
}
