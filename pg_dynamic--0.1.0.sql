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

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_dynamic" to load this file. \quit

CREATE TYPE dynamic;

CREATE FUNCTION dynamic_in(cstring)
RETURNS dynamic
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION dynamic_out(dynamic) 
RETURNS cstring 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

-- binary I/O functions
CREATE FUNCTION dynamic_send(dynamic) 
RETURNS bytea 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION dynamic_recv(internal) 
RETURNS dynamic 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE TYPE dynamic (
    INPUT = dynamic_in,
    OUTPUT = dynamic_out,
    SEND = dynamic_send,
    RECEIVE = dynamic_recv,
    LIKE = jsonb,
    STORAGE = extended
);


/*
 * Typecasting
 */
CREATE FUNCTION dynamic_to_int8(dynamic) RETURNS bigint
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_to_int8';

CREATE CAST (dynamic as int8) WITH FUNCTION dynamic_to_int8(dynamic);

CREATE FUNCTION int8_to_dynamic(bigint) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'int8_to_dynamic';

CREATE CAST (int8 as dynamic) WITH FUNCTION int8_to_dynamic(int8);

CREATE FUNCTION dynamic_tobigint(dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_tobigint';

CREATE FUNCTION dynamic_to_inet(dynamic) RETURNS inet
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_to_inet';

CREATE CAST (dynamic as inet) WITH FUNCTION dynamic_to_inet(dynamic);

CREATE FUNCTION inet_to_dynamic(inet) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'inet_to_dynamic';

CREATE CAST (inet as dynamic) WITH FUNCTION inet_to_dynamic(inet);

CREATE FUNCTION dynamic_toinet(dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_toinet';


CREATE FUNCTION dynamic_to_box(dynamic) RETURNS box
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_to_box';

CREATE CAST (dynamic as box) WITH FUNCTION dynamic_to_box(dynamic);

CREATE FUNCTION box_to_dynamic(box) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'box_to_dynamic';

CREATE CAST (box as dynamic) WITH FUNCTION box_to_dynamic(box);

CREATE FUNCTION dynamic_tobox(dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_tobox';

--
-- Operators
--
CREATE FUNCTION dynamic_add(dynamic, dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_add';

CREATE OPERATOR + (
    FUNCTION = dynamic_add,
    LEFTARG = dynamic,
    RIGHTARG = dynamic
);

CREATE FUNCTION dynamic_uplus(dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_uplus';

CREATE OPERATOR + (
    FUNCTION = dynamic_uplus,
    RIGHTARG = dynamic
);

CREATE FUNCTION dynamic_uminus(dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_uminus';

CREATE OPERATOR - (
    FUNCTION = dynamic_uminus,
    RIGHTARG = dynamic
);


CREATE FUNCTION dynamic_sub(dynamic, dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_sub';

CREATE OPERATOR - (
    FUNCTION = dynamic_sub,
    LEFTARG = dynamic,
    RIGHTARG = dynamic
);

CREATE FUNCTION dynamic_mul(dynamic, dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_mul';

CREATE OPERATOR * (
    FUNCTION = dynamic_mul,
    LEFTARG = dynamic,
    RIGHTARG = dynamic
);

CREATE FUNCTION dynamic_div(dynamic, dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_div';

CREATE OPERATOR / (
    FUNCTION = dynamic_div,
    LEFTARG = dynamic,
    RIGHTARG = dynamic
);

/*
CREATE FUNCTION dynamic_overlap(dynamic, dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_overlap';

CREATE OPERATOR && (
    FUNCTION = dynamic_overlap,
    LEFTARG = dynamic,
    RIGHTARG = dynamic
);
*/


--
-- Number Functions
--
CREATE FUNCTION abs(dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_abs';

CREATE FUNCTION gcd(dynamic, dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_gcd';

CREATE FUNCTION lcm(dynamic, dynamic) RETURNS dynamic
LANGUAGE C IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'dynamic_lcm';