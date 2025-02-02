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
-- dynamic data type regression tests
--
--
-- Load extension and set path
--
SET extra_float_digits = 0;
set timezone TO 'GMT';
--
-- Create a table using the DYNAMIC type
--
CREATE TABLE dynamic_table (type text, dynamic dynamic);
--
-- Insert values to exercise dynamic_in/dynamic_out
--
INSERT INTO dynamic_table VALUES ('bool', 'true');
INSERT INTO dynamic_table VALUES ('bool', 'false');
INSERT INTO dynamic_table VALUES ('null', 'null');
INSERT INTO dynamic_table VALUES ('string', '""');
INSERT INTO dynamic_table VALUES ('string', '"This is a string"');
INSERT INTO dynamic_table VALUES ('integer', '0');
INSERT INTO dynamic_table VALUES ('integer', '9223372036854775807');
INSERT INTO dynamic_table VALUES ('integer', '-9223372036854775808');
INSERT INTO dynamic_table VALUES ('float', '0.0');
INSERT INTO dynamic_table VALUES ('float', '1.0');
INSERT INTO dynamic_table VALUES ('float', '-1.0');
INSERT INTO dynamic_table VALUES ('float', '100000000.000001');
INSERT INTO dynamic_table VALUES ('float', '-100000000.000001');
INSERT INTO dynamic_table VALUES ('float', '0.00000000000000012345');
INSERT INTO dynamic_table VALUES ('float', '-0.00000000000000012345');
INSERT INTO dynamic_table VALUES ('numeric', '100000000000.0000000000001::numeric');
INSERT INTO dynamic_table VALUES ('numeric', '-100000000000.0000000000001::numeric');
INSERT INTO dynamic_table VALUES ('timestamp', '"2023-06-23 13:39:40.00"::timestamp');
INSERT INTO dynamic_table VALUES ('timestamp', '"06/23/2023 13:39:40.00"::timestamp');
INSERT INTO dynamic_table VALUES ('timestamp', '"Fri Jun 23 13:39:40.00 2023"::timestamp');
INSERT INTO dynamic_table VALUES ('timestamptz', '"1997-12-17 07:37:16-06"::timestamptz');
INSERT INTO dynamic_table VALUES ('timestamptz', '"12/17/1997 07:37:16.00+00"::timestamptz');
INSERT INTO dynamic_table VALUES ('timestamptz', '"Wed Dec 17 07:37:16 1997+09"::timestamptz');
INSERT INTO dynamic_table VALUES ('date', '"1997-12-17"::date');
INSERT INTO dynamic_table VALUES ('date', '"12/17/1997"::date');
INSERT INTO dynamic_table VALUES ('date', '"Wed Dec 17 1997"::date');
INSERT INTO dynamic_table VALUES ('time', '"07:37:16-08"::time');
INSERT INTO dynamic_table VALUES ('time', '"07:37:16.00"::time');
INSERT INTO dynamic_table VALUES ('time', '"07:37:16"::time');
INSERT INTO dynamic_table VALUES ('timetz', '"07:37:16-08"::timetz');
INSERT INTO dynamic_table VALUES ('timetz', '"07:37:16.00"::timetz');
INSERT INTO dynamic_table VALUES ('timetz', '"07:37:16"::timetz');
INSERT INTO dynamic_table VALUES ('interval', '"30 Seconds"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"15 Minutes"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"10 Hours"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"40 Days"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"10 Weeks"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"10 Months"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"3 Years"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"30 Seconds Ago"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"15 Minutes Ago"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"10 Hours Ago"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"40 Days Ago"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"10 Weeks Ago"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"10 Months Ago"::interval');
INSERT INTO dynamic_table VALUES ('interval', '"3 Years Ago"::interval');
INSERT INTO dynamic_table VALUES ('inet', '"192.168.1.5"::inet');
INSERT INTO dynamic_table VALUES ('inet', '"192.168.1/24"::inet');
INSERT INTO dynamic_table VALUES ('inet', '"::ffff:fff0:1"::inet');
INSERT INTO dynamic_table VALUES ('inet', '"192.168.1.5"::cidr');
INSERT INTO dynamic_table VALUES ('inet', '"192.168.1/24"::cidr');
INSERT INTO dynamic_table VALUES ('inet', '"::ffff:fff0:1"::cidr');
INSERT INTO dynamic_table VALUES ('integer array',
	'[-9223372036854775808, -1, 0, 1, 9223372036854775807]');
INSERT INTO dynamic_table VALUES('float array',
	'[-0.00000000000000012345, -100000000.000001, -1.0, 0.0, 1.0, 100000000.000001, 0.00000000000000012345]');
INSERT INTO dynamic_table VALUES('mixed array', '[true, false, null, "string", 1, 1.0, {"bool":true}, -1::numeric, [1,3,5]]');
INSERT INTO dynamic_table VALUES('object', '{"bool":true, "null":null, "string":"string", "integer":1, "float":1.2, "arrayi":[-1,0,1], "arrayf":[-1.0, 0.0, 1.0], "object":{"bool":true, "null":null, "string":"string", "int":1, "float":8.0}}');
INSERT INTO dynamic_table VALUES ('numeric array',
        '[-5::numeric, -1::numeric, 0::numeric, 1::numeric, 9223372036854775807::numeric]');
--
-- Special float values: NaN, +/- Infinity
--
INSERT INTO dynamic_table VALUES ('float  nan', 'nan');
INSERT INTO dynamic_table VALUES ('float  Infinity', 'Infinity');
INSERT INTO dynamic_table VALUES ('float -Infinity', '-Infinity');
INSERT INTO dynamic_table VALUES ('float  inf', 'inf');
INSERT INTO dynamic_table VALUES ('float -inf', '-inf');
SELECT * FROM dynamic_table;
      type       |                                                                                                       dynamic                                                                                                        
-----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 bool            | true
 bool            | false
 null            | null
 string          | ""
 string          | "This is a string"
 integer         | 0
 integer         | 9223372036854775807
 integer         | -9223372036854775808
 float           | 0.0
 float           | 1.0
 float           | -1.0
 float           | 100000000.000001
 float           | -100000000.000001
 float           | 1.2345e-16
 float           | -1.2345e-16
 numeric         | 100000000000.0000000000001::numeric
 numeric         | -100000000000.0000000000001::numeric
 timestamp       | Fri Jun 23 13:39:40 2023
 timestamp       | Fri Jun 23 13:39:40 2023
 timestamp       | Fri Jun 23 13:39:40 2023
 timestamptz     | Wed Dec 17 13:37:16 1997 GMT
 timestamptz     | Wed Dec 17 07:37:16 1997 GMT
 timestamptz     | Tue Dec 16 22:37:16 1997 GMT
 date            | 12-17-1997
 date            | 12-17-1997
 date            | 12-17-1997
 time            | 07:37:16
 time            | 07:37:16
 time            | 07:37:16
 timetz          | 07:37:16-08
 timetz          | 07:37:16+00
 timetz          | 07:37:16+00
 interval        | @ 30 secs
 interval        | @ 15 mins
 interval        | @ 10 hours
 interval        | @ 40 days
 interval        | @ 70 days
 interval        | @ 10 mons
 interval        | @ 3 years
 interval        | @ 30 secs ago
 interval        | @ 15 mins ago
 interval        | @ 10 hours ago
 interval        | @ 40 days ago
 interval        | @ 70 days ago
 interval        | @ 10 mons ago
 interval        | @ 3 years ago
 inet            | 192.168.1.5
 inet            | 192.168.1.0/24
 inet            | ::ffff:255.240.0.1
 inet            | 192.168.1.5/32
 inet            | 192.168.1.0/24
 inet            | ::ffff:255.240.0.1/128
 integer array   | [-9223372036854775808, -1, 0, 1, 9223372036854775807]
 float array     | [-1.2345e-16, -100000000.000001, -1.0, 0.0, 1.0, 100000000.000001, 1.2345e-16]
 mixed array     | [true, false, null, "string", 1, 1.0, {"bool": true}, -1::numeric, [1, 3, 5]]
 object          | {"bool": true, "null": null, "float": 1.2, "arrayf": [-1.0, 0.0, 1.0], "arrayi": [-1, 0, 1], "object": {"int": 1, "bool": true, "null": null, "float": 8.0, "string": "string"}, "string": "string", "integer": 1}
 numeric array   | [-5::numeric, -1::numeric, 0::numeric, 1::numeric, 9223372036854775807::numeric]
 float  nan      | NaN
 float  Infinity | Infinity
 float -Infinity | -Infinity
 float  inf      | Infinity
 float -inf      | -Infinity
(62 rows)

--
-- These should fail
--
INSERT INTO dynamic_table VALUES ('bad integer', '9223372036854775808');
ERROR:  value "9223372036854775808" is out of range for type bigint
LINE 1: INSERT INTO dynamic_table VALUES ('bad integer', '922337203685...
                                                       ^
INSERT INTO dynamic_table VALUES ('bad integer', '-9223372036854775809');
ERROR:  value "-9223372036854775809" is out of range for type bigint
LINE 1: INSERT INTO dynamic_table VALUES ('bad integer', '-92233720368...
                                                       ^
INSERT INTO dynamic_table VALUES ('bad float', '-NaN');
ERROR:  invalid input syntax for type dynamic
LINE 1: INSERT INTO dynamic_table VALUES ('bad float', '-NaN');
                                                     ^
DETAIL:  Token "-NaN" is invalid.
CONTEXT:  dynamic data, line 1: -NaN
INSERT INTO dynamic_table VALUES ('bad float', 'Infi');
ERROR:  invalid input syntax for type dynamic
LINE 1: INSERT INTO dynamic_table VALUES ('bad float', 'Infi');
                                                     ^
DETAIL:  Expected dynamic value, but found "Infi".
CONTEXT:  dynamic data, line 1: Infi
INSERT INTO dynamic_table VALUES ('bad float', '-Infi');
ERROR:  invalid input syntax for type dynamic
LINE 1: INSERT INTO dynamic_table VALUES ('bad float', '-Infi');
                                                     ^
DETAIL:  Token "-Infi" is invalid.
CONTEXT:  dynamic data, line 1: -Infi
