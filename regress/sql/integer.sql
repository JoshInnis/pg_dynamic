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

/*
 * Bigint Typecasting
 */
 SELECT ('1'::dynamic)::bigint;
 SELECT ('"1"'::dynamic)::bigint;
 SELECT ('1.0'::dynamic)::bigint;
 SELECT ('1::numeric'::dynamic)::bigint;

 SELECT (1::bigint)::dynamic;

SELECT dynamic_tobigint('1');
SELECT dynamic_tobigint('"1"');
SELECT dynamic_tobigint('1.0');
SELECT dynamic_tobigint('1::numeric');
SELECT dynamic_tobigint('true');
SELECT dynamic_tobigint('false');
SELECT dynamic_tobigint('null');
SELECT dynamic_tobigint('"2023-06-23 13:39:40.00"::timestamp');
SELECT dynamic_tobigint('"1997-12-17 07:37:16-06"::timestamptz');
SELECT dynamic_tobigint('"1997-12-17"::date');
SELECT dynamic_tobigint('"07:37:16-08"::time');
SELECT dynamic_tobigint('"07:37:16"::timetz');
SELECT dynamic_tobigint('"30 Seconds"::interval');
SELECT dynamic_tobigint('"192.168.1.5"::inet');
SELECT dynamic_tobigint('"192.168.1.5"::cidr');
SELECT dynamic_tobigint('[-1, 0, 1]');
SELECT dynamic_tobigint('{"bool":true, "null":null, "string":"string", "integer":1, "float":1.2, "arrayi":[-1,0,1], "arrayf":[-1.0, 0.0, 1.0], "object":{"bool":true, "null":null, "string":"string", "int":1, "float":8.0}}');

SELECT dynamic_tobigint('nan');
SELECT dynamic_tobigint('Infinity');
SELECT dynamic_tobigint('-Infinity');
SELECT dynamic_tobigint('inf');
SELECT dynamic_tobigint('-inf');

--
-- Operators
--
SELECT '10'::dynamic + '100'::dynamic;
SELECT '"60"'::dynamic + '100'::dynamic;
SELECT '1.0'::dynamic + '100'::dynamic;
SELECT '50::numeric'::dynamic + '100'::dynamic;
SELECT '-1'::dynamic + '100'::dynamic;

SELECT '10'::dynamic - '100'::dynamic;
SELECT '"60"'::dynamic - '100'::dynamic;
SELECT '1.0'::dynamic - '100'::dynamic;
SELECT '50::numeric'::dynamic - '100'::dynamic;
SELECT '-1'::dynamic - '100'::dynamic;


SELECT '10'::dynamic * '100'::dynamic;
SELECT '"60"'::dynamic * '100'::dynamic;
SELECT '1.0'::dynamic * '100'::dynamic;
SELECT '50::numeric'::dynamic * '100'::dynamic;
SELECT '-1'::dynamic * '100'::dynamic;

--
-- Functions
--
 SELECT abs('1'::dynamic);
 SELECT abs('"1"'::dynamic);
 SELECT abs('1.0'::dynamic);
 SELECT abs('1::numeric'::dynamic);
 SELECT abs('-1'::dynamic);
 SELECT abs('0'::dynamic);


 SELECT gcd('10'::dynamic, '100'::dynamic);
 SELECT gcd('"60"'::dynamic, '100'::dynamic);
 SELECT gcd('1.0'::dynamic, '100'::dynamic);
 SELECT gcd('50::numeric'::dynamic, '100'::dynamic);
 SELECT gcd('-1'::dynamic, '100'::dynamic);
 SELECT gcd('101'::dynamic, '100'::dynamic);

 SELECT lcm('10'::dynamic, '100'::dynamic);
 SELECT lcm('"60"'::dynamic, '100'::dynamic);
 SELECT lcm('1.0'::dynamic, '100'::dynamic);
 SELECT lcm('50::numeric'::dynamic, '100'::dynamic);
 SELECT lcm('-1'::dynamic, '100'::dynamic);
 SELECT lcm('101'::dynamic, '100'::dynamic);
