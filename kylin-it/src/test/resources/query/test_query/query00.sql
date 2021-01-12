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
count(LSTG_FORMAT_NAME),
sum(price),
sum(0.9*(price+50)) as gmv,
sum(case when LSTG_FORMAT_NAME is null then 0 else price end),
sum(case
    when LSTG_FORMAT_NAME = 'ABIN' AND LSTG_FORMAT_NAME = 'FP-GTC' then 2*price + ITEM_COUNT
    when LSTG_FORMAT_NAME = 'Auction' then (1+2)*price*(2+3)+(2+3)*(3+2)*(4+5)-4+5
    else 3
    end)
FROM test_kylin_fact
group by SLR_SEGMENT_CD, (case
                             when SLR_SEGMENT_CD < 0 then 0
                             when SLR_SEGMENT_CD < 10 then 1
                             else 3
                             end)
order by SLR_SEGMENT_CD
;{"scanRowCount":300,"scanBytes":0,"scanFiles":1,"cuboidId":[14336]}