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


/*
 * Typecasting
 */
PG_FUNCTION_INFO_V1(dynamic_to_int8);
Datum
dynamic_to_int8(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    PG_RETURN_INT64(DatumGetInt64(convert_to_scalar(dynamic_to_int8_internal, agt, "dynamic integer")));
    

}

PG_FUNCTION_INFO_V1(int8_to_dynamic);
Datum
int8_to_dynamic(PG_FUNCTION_ARGS) {
   dynamic_value gtv = {
        .type = DYNAMIC_INTEGER,
        .val.int_value = PG_GETARG_INT64(0)
    };

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tobigint);
Datum
dynamic_tobigint(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    dynamic_value gtv = {
        .type = DYNAMIC_INTEGER,
        .val.int_value = DatumGetInt64(convert_to_scalar(dynamic_to_int8_internal, agt, "dynamic integer"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

Datum
dynamic_to_int8_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTEGER)
        return Int64GetDatum(gtv->val.int_value);
    else if (gtv->type == DYNAMIC_FLOAT)
        return DirectFunctionCall1Coll(dtoi8, InvalidOid, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == DYNAMIC_NUMERIC)
        return DirectFunctionCall1Coll(numeric_int8, InvalidOid, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1Coll(int8in, InvalidOid, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "int8");

    // cannot reach
    return 0;
}



PG_FUNCTION_INFO_V1(dynamic_abs);
Datum
dynamic_abs(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    dynamic_value gtv = {
        .type = DYNAMIC_INTEGER,
        .val.int_value = DirectFunctionCall1(int8abs,
           convert_to_scalar(dynamic_to_int8_internal, agt, "dynamic integer"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}


PG_FUNCTION_INFO_V1(dynamic_gcd);
Datum
dynamic_gcd(PG_FUNCTION_ARGS) {
    dynamic *agt_0 = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *agt_1 = AG_GET_ARG_DYNAMIC_P(1);

    dynamic_value gtv = {
        .type = DYNAMIC_INTEGER,
        .val.int_value = DatumGetUInt64(DirectFunctionCall2(int8gcd,
           convert_to_scalar(dynamic_to_int8_internal, agt_0, "dynamic integer"),
           convert_to_scalar(dynamic_to_int8_internal, agt_1, "dynamic integer")))
    };

    PG_FREE_IF_COPY(agt_0, 0);
    PG_FREE_IF_COPY(agt_1, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}


