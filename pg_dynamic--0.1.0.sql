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
