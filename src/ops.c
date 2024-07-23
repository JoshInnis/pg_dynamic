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

#include "postgres.h"

#include <math.h>

#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "varatt.h"

#include "utils/dynamic.h"
#include "dynamic_typecasting.h"

PG_FUNCTION_INFO_V1(dynamic_add);
Datum
dynamic_add(PG_FUNCTION_ARGS) {
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);

    dynamic_value dyna_val = {
        .type = DYNAMIC_INTEGER,
        .val.int_value = DatumGetUInt64(DirectFunctionCall2(int8pl,
           convert_to_scalar(dynamic_to_int8_internal, lhs, "dynamic integer"),
           convert_to_scalar(dynamic_to_int8_internal, rhs, "dynamic integer")))
    };

    PG_FREE_IF_COPY(lhs, 0);
    PG_FREE_IF_COPY(rhs, 0);
    
    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&dyna_val));    
}