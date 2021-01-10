--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

select SLR_SEGMENT_CD,
(case
    when SLR_SEGMENT_CD < 0 then 0
    when SLR_SEGMENT_CD < 10 then 1
    else 3
    end),
sum(price),
sum(case when LSTG_FORMAT_NAME is null then 0 else price end)
FROM test_kylin_fact
group by SLR_SEGMENT_CD, (case
                             when SLR_SEGMENT_CD < 0 then 0
                             when SLR_SEGMENT_CD < 10 then 1
                             else 3
                             end)
order by SLR_SEGMENT_CD
;{"scanRowCount":300,"scanBytes":0,"scanFiles":1,"cuboidId":[14336]}