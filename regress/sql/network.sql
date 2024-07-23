/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

--
-- Typecasting
--
SELECT ('192.168.1.5'::inet)::dynamic;
SELECT ('192.168.0.5'::inet)::dynamic;
SELECT ('192.168.1/24'::inet)::dynamic;
--

SELECT ('"192.168.1.5"::inet'::dynamic)::inet;
SELECT ('"192.168.0.5"::inet'::dynamic)::inet;
SELECT ('"192.168.1/24"::inet'::dynamic)::inet;


SELECT dynamic_toinet('"192.168.1/24"::inet');
SELECT dynamic_toinet('"192.168.1/24"');
SELECT dynamic_toinet('1');
SELECT dynamic_toinet('"1"');
SELECT dynamic_toinet('1.0');
SELECT dynamic_toinet('1::numeric');
SELECT dynamic_toinet('true');
SELECT dynamic_toinet('false');
SELECT dynamic_toinet('null');
SELECT dynamic_toinet('"2023-06-23 13:39:40.00"::timestamp');
SELECT dynamic_toinet('"1997-12-17 07:37:16-06"::timestamptz');
SELECT dynamic_toinet('"1997-12-17"::date');
SELECT dynamic_toinet('"07:37:16-08"::time');
SELECT dynamic_toinet('"07:37:16"::timetz');
SELECT dynamic_toinet('"30 Seconds"::interval');
SELECT dynamic_toinet('"192.168.1.5"::inet');
SELECT dynamic_toinet('"192.168.1.5"::cidr');
SELECT dynamic_toinet('[-1, 0, 1]');
SELECT dynamic_toinet('{"bool":true, "null":null, "string":"string", "integer":1, "float":1.2, "arrayi":[-1,0,1], "arrayf":[-1.0, 0.0, 1.0], "object":{"bool":true, "null":null, "string":"string", "int":1, "float":8.0}}');


--
-- inet && inet
--
SELECT '"192.168.1.5"::inet'::dynamic && '192.168.1/24::inet'::dynamic;
SELECT '"192.168.0.5"::inet'::dynamic && '"192.168.1/24"::inet'::dynamic;
SELECT '"192.168.1/24"::inet'::dynamic && '"192.168.1/24"::inet'::dynamic;