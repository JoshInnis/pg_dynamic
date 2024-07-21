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
//#include "utils/int8.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"
#include "utils/palloc.h"

#include "utils/dynamic.h"
#include "dynamic_typecasting.h"

Datum convert_to_scalar(coearce_function func, dynamic *agt, char *type) {
    if (!DYNA_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot cast non-scalar dynamic to %s", type)));

    dynamic_value *gtv = get_ith_dynamic_value_from_container(&agt->root, 0);

    Datum d = func(gtv);

    return d;
}

/*
 * Emit correct, translatable cast error message
 */
void
cannot_cast_dynamic_value(enum dynamic_value_type type, const char *sqltype) {
    static const struct {
        enum dynamic_value_type type;
        const char *msg;
    } messages[] = {
        {DYNAMIC_NULL, gettext_noop("cannot cast dynamic null to type %s")},
        {DYNAMIC_STRING, gettext_noop("cannot cast dynamic string to type %s")},
        {DYNAMIC_NUMERIC, gettext_noop("cannot cast dynamic numeric to type %s")},
        {DYNAMIC_INTEGER, gettext_noop("cannot cast dynamic integer to type %s")},
        {DYNAMIC_FLOAT, gettext_noop("cannot cast dynamic float to type %s")},
        {DYNAMIC_BOOL, gettext_noop("cannot cast dynamic boolean to type %s")},
    	{DYNAMIC_TIMESTAMP, gettext_noop("cannot cast dynamic timestamp to type %s")},
	    {DYNAMIC_TIMESTAMPTZ, gettext_noop("cannot cast dynamic timestamptz to type %s")},
	    {DYNAMIC_DATE, gettext_noop("cannot cast dynamic date to type %s")},
	    {DYNAMIC_TIME, gettext_noop("cannot cast dynamic time to type %s")},
	    {DYNAMIC_TIMETZ, gettext_noop("cannot cast dynamic timetz to type %s")},
        {DYNAMIC_INTERVAL, gettext_noop("cannot cast dynamic interval to type %s")},
	    {DYNAMIC_ARRAY, gettext_noop("cannot cast dynamic array to type %s")},
        {DYNAMIC_OBJECT, gettext_noop("cannot cast dynamic object to type %s")},
        {DYNAMIC_BINARY, gettext_noop("cannot cast dynamic array or object to type %s")}};

    for (int i = 0; i < lengthof(messages); i++) {
        if (messages[i].type == type) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg(messages[i].msg, sqltype)));
        }
    }

    elog(ERROR, "unknown dynamic type: %d can't cast to %s", (int)type, sqltype);
}
