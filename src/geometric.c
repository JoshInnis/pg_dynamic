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

#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation_d.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/palloc.h"
#include "fmgr.h"
#include "utils/fmgrprotos.h"

#include "utils/dynamic.h"

#include "dynamic_typecasting.h"

PG_FUNCTION_INFO_V1(dynamic_to_box);
Datum 
dynamic_to_box(PG_FUNCTION_ARGS) {
    dynamic *dyna = AG_GET_ARG_DYNAMIC_P(0);

    PG_RETURN_BOX_P(convert_to_scalar(dynamic_to_box_internal, dyna, "box"));
}

PG_FUNCTION_INFO_V1(box_to_dynamic);
Datum
box_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value dynav = {
        .type = DYNAMIC_BOX,
        .val.box = PG_GETARG_BOX_P(0)
    };

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&dynav));
}

PG_FUNCTION_INFO_V1(dynamic_tobox);
Datum
dynamic_tobox(PG_FUNCTION_ARGS) {
    dynamic *dyna = AG_GET_ARG_DYNAMIC_P(0);

    dynamic_value dynav = {
        .type = DYNAMIC_BOX,
        .val.box = DatumGetPointer(convert_to_scalar(dynamic_to_box_internal, dyna, "box"))
    };

    PG_FREE_IF_COPY(dyna, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&dynav));
}

Datum
dynamic_to_box_internal(dynamic_value *dyna) {
    if (dyna->type == DYNAMIC_BOX) {
        return PointerGetDatum(dyna->val.box);
    } else if (dyna->type == DYNAMIC_STRING) {
        return DirectFunctionCall1(box_in, CStringGetDatum(dyna->val.string.val));
    }  else
        cannot_cast_dynamic_value(dyna->type, "box");

    // unreachable
    return CStringGetDatum("");
}

