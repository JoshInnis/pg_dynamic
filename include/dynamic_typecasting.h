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

#ifndef DYNA_TYPECASTING_H
#define DYNA_TYPECASTING_H

#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation_d.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
//#include "utils/int8.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"
#include "utils/palloc.h"

#include "utils/dynamic.h"

#include "fmgr.h"
#include "utils/timestamp.h"
#include "utils/dynamic.h"

void cannot_cast_dynamic_value(enum dynamic_value_type type, const char *sqltype);

typedef Datum (*coearce_function) (dynamic_value *);
Datum convert_to_scalar(coearce_function func, dynamic *agt, char *type);

Datum dynamic_to_int8_internal(dynamic_value *gtv);
Datum dynamic_to_inet_internal(dynamic_value *gtv);

#endif
