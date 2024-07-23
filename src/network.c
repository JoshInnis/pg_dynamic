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

PG_FUNCTION_INFO_V1(dynamic_to_inet);
Datum 
dynamic_to_inet(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    PG_RETURN_INET_P(convert_to_scalar(dynamic_to_inet_internal, agt, "inet"));
}

PG_FUNCTION_INFO_V1(inet_to_dynamic);
Datum
inet_to_dynamic(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);

    dynamic_value gtv;
    gtv.type = DYNAMIC_INET;
    memcpy(&gtv.val.inet, ip, sizeof(char) * 22);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_toinet);
Datum
dynamic_toinet(PG_FUNCTION_ARGS) {
    dynamic *dyna = AG_GET_ARG_DYNAMIC_P(0);

    inet *i = DatumGetInetP(convert_to_scalar(dynamic_to_inet_internal, dyna, "dynamic inet"));
    dynamic_value dynav;
    dynav.type = DYNAMIC_INET;
    memcpy(&dynav.val.inet, i, sizeof(char) * 22);


    PG_FREE_IF_COPY(dyna, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&dynav));
}


Datum
dynamic_to_inet_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INET)
        return InetPGetDatum(&gtv->val.inet);
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(inet_in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "inet");

    // unreachable
    return CStringGetDatum("");
}
