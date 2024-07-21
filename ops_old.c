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
 * Functions for operators in Cypher expressions.
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
#include "utils/dynamic_typecasting.h"

static void ereport_op_str(const char *op, dynamic *lhs, dynamic *rhs);
static dynamic *dynamic_concat(dynamic *agt1, dynamic *agt2);
static dynamic_value *iterator_concat(dynamic_iterator **it1,
                                     dynamic_iterator **it2,
                                     dynamic_parse_state **state);
static void concat_to_dynamic_string(dynamic_value *result, char *lhs, int llen,
                                    char *rhs, int rlen);
static char *get_string_from_dynamic_value(dynamic_value *agtv, int *length);

static void concat_to_dynamic_string(dynamic_value *result, char *lhs, int llen,
                                    char *rhs, int rlen)
{
    int length = llen + rlen;
    char *buffer = result->val.string.val;

    Assert(llen >= 0 && rlen >= 0);
    check_string_length(length);
    buffer = palloc(length);

    strncpy(buffer, lhs, llen);
    strncpy(buffer + llen, rhs, rlen);

    result->type = DYNAMIC_STRING;
    result->val.string.len = length;
    result->val.string.val = buffer;
}

static char *get_string_from_dynamic_value(dynamic_value *agtv, int *length)
{
    Datum number;
    char *string;

    switch (agtv->type)
    {
    case DYNAMIC_INTEGER:
        number = DirectFunctionCall1(int8out,
                                     Int8GetDatum(agtv->val.int_value));
        string = DatumGetCString(number);
        *length = strlen(string);
        return string;
    case DYNAMIC_FLOAT:
        number = DirectFunctionCall1(float8out,
                                     Float8GetDatum(agtv->val.float_value));
        string = DatumGetCString(number);
        *length = strlen(string);

        if (is_decimal_needed(string))
        {
            char *str = palloc(*length + 2);
            strncpy(str, string, *length);
            strncpy(str + *length, ".0", 2);
            *length += 2;
            string = str;
        }
        return string;
    case DYNAMIC_STRING:
        *length = agtv->val.string.len;
        return agtv->val.string.val;

    case DYNAMIC_NUMERIC:
        string = DatumGetCString(DirectFunctionCall1(numeric_out,
                     PointerGetDatum(agtv->val.numeric)));
        *length = strlen(string);
        return string;

    case DYNAMIC_NULL:
    case DYNAMIC_BOOL:
    case DYNAMIC_ARRAY:
    case DYNAMIC_OBJECT:
    case DYNAMIC_BINARY:
    default:
        *length = 0;
        return NULL;
    }
    return NULL;
}

Datum get_numeric_datum_from_dynamic_value(dynamic_value *agtv)
{
    switch (agtv->type)
    {
    case DYNAMIC_INTEGER:
        return DirectFunctionCall1(int8_numeric,
                                   Int8GetDatum(agtv->val.int_value));
    case DYNAMIC_FLOAT:
        return DirectFunctionCall1(float8_numeric,
                                   Float8GetDatum(agtv->val.float_value));
    case DYNAMIC_NUMERIC:
        return NumericGetDatum(agtv->val.numeric);

    default:
        break;
    }

    return 0;
}

bool is_numeric_result(dynamic_value *lhs, dynamic_value *rhs)
{
    if (((lhs->type == DYNAMIC_NUMERIC || rhs->type == DYNAMIC_NUMERIC) &&
         (lhs->type == DYNAMIC_INTEGER || lhs->type == DYNAMIC_FLOAT ||
          rhs->type == DYNAMIC_INTEGER || rhs->type == DYNAMIC_FLOAT )) ||
        (lhs->type == DYNAMIC_NUMERIC && rhs->type == DYNAMIC_NUMERIC))
        return true;
    return false;
}

bool is_gtv_number(dynamic_value *gt)
{
    if (gt->type == DYNAMIC_NUMERIC || gt->type == DYNAMIC_INTEGER || gt->type == DYNAMIC_FLOAT)
        return true;
    return false;
}


PG_FUNCTION_INFO_V1(dynamic_add);

/* dynamic addition and concat function for + operator */
Datum dynamic_add(PG_FUNCTION_ARGS)
{
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value *agtv_lhs;
    dynamic_value *agtv_rhs;
    dynamic_value agtv_result;

    /* If both are not scalars */
    if (!(DYNA_ROOT_IS_SCALAR(lhs) && DYNA_ROOT_IS_SCALAR(rhs))) {

        /* It can't be a scalar and an object */
        if ((DYNA_ROOT_IS_SCALAR(lhs) && DYNA_ROOT_IS_OBJECT(rhs)) ||
	    (DYNA_ROOT_IS_OBJECT(lhs) && DYNA_ROOT_IS_SCALAR(rhs)) ||
            (DYNA_ROOT_IS_OBJECT(lhs) && DYNA_ROOT_IS_OBJECT(rhs)))
            ereport_op_str("+", lhs, rhs);

        AG_RETURN_DYNAMIC_P(dynamic_concat(lhs, rhs));
    }

    /* Both are scalar */
    agtv_lhs = get_ith_dynamic_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_dynamic_value_from_container(&rhs->root, 0);

    // Concat Strings and Strings with Numbers
    if ((agtv_lhs->type == DYNAMIC_STRING && (is_gtv_number(agtv_rhs) || agtv_rhs->type == DYNAMIC_STRING)) || 
        (agtv_rhs->type == DYNAMIC_STRING && (is_gtv_number(agtv_lhs) || agtv_lhs->type == DYNAMIC_STRING)))
    {
	char *lhs_str = GT_TO_STRING(lhs);
        int llen = strlen(lhs_str);
        char *rhs_str = GT_TO_STRING(rhs);
	int rlen = strlen(rhs_str);

        concat_to_dynamic_string(&agtv_result, lhs_str, llen, rhs_str, rlen);
    }
    else if (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INTEGER;
        agtv_result.val.int_value = GT_TO_INT8(lhs) + GT_TO_INT8(rhs);
    } else if ((agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_FLOAT) ||
               (agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_INTEGER) ||
               (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_FLOAT)) {
        agtv_result.type = DYNAMIC_FLOAT;
        agtv_result.val.float_value = GT_TO_FLOAT8(lhs) + GT_TO_FLOAT8(rhs);
    } else if (is_numeric_result(agtv_lhs, agtv_rhs)) {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_dynamic_value(agtv_lhs);
        rhsd = get_numeric_datum_from_dynamic_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_add, lhsd, rhsd);

        agtv_result.type = DYNAMIC_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    } else if (agtv_lhs->type == DYNAMIC_TIMESTAMP && agtv_rhs->type == DYNAMIC_INTERVAL) {
        agtv_result.type = DYNAMIC_TIMESTAMP;
	agtv_result.val.int_value = DatumGetTimestamp(DirectFunctionCall2(timestamp_pl_interval,
				TimestampGetDatum(agtv_lhs->val.int_value),
				IntervalPGetDatum(&agtv_rhs->val.interval)));
    } else if (agtv_rhs->type == DYNAMIC_INTERVAL) {
	Datum i = IntervalPGetDatum(&agtv_rhs->val.interval);
	agtv_result.type = agtv_lhs->type;

        if (agtv_lhs->type == DYNAMIC_TIMESTAMPTZ) {
            agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
                                    TimestampTzGetDatum(agtv_lhs->val.int_value), i));
        } else if (agtv_lhs->type == DYNAMIC_DATE) {
	     agtv_result.type = DYNAMIC_TIMESTAMPTZ;
             agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(date_pl_interval,
                                    DateADTGetDatum(agtv_lhs->val.date), i));
        }   else if (agtv_lhs->type == DYNAMIC_TIME) {
            agtv_result.val.int_value = DatumGetTimeADT(DirectFunctionCall2(time_pl_interval,
                                    TimeADTGetDatum(agtv_lhs->val.int_value), i));
        } else if (agtv_lhs->type == DYNAMIC_TIMETZ) {
            TimeTzADT *time = DatumGetTimeTzADTP(DirectFunctionCall2(timetz_pl_interval,
                                    TimeTzADTPGetDatum(&agtv_lhs->val.timetz), i));

            agtv_result.val.timetz.time = time->time;
            agtv_result.val.timetz.zone = time->zone;
        } else if (agtv_lhs->type == DYNAMIC_INTERVAL) {
            Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_pl,
                                    IntervalPGetDatum(&agtv_lhs->val.interval), i));

            agtv_result.val.interval.time = interval->time;
            agtv_result.val.interval.day = interval->day;
            agtv_result.val.interval.month = interval->month;
        } else {
            ereport_op_str("+", lhs, rhs);
       }
    } else if (agtv_rhs->type == DYNAMIC_POINT) {
        Datum point = PointPGetDatum(agtv_rhs->val.point);
        agtv_result.type = agtv_lhs->type;
	if (agtv_lhs->type == DYNAMIC_POINT) {
            agtv_result.val.point = DatumGetPointP(DirectFunctionCall2(point_add, PointPGetDatum(agtv_lhs->val.point), point)); 
	} else if (agtv_lhs->type == DYNAMIC_BOX) {
            agtv_result.val.box = DatumGetBoxP(DirectFunctionCall2(box_add, BoxPGetDatum(agtv_lhs->val.box), point));    
        } else if (agtv_lhs->type == DYNAMIC_PATH) {
            agtv_result.val.path = DatumGetPathP(DirectFunctionCall2(path_add_pt, PathPGetDatum(agtv_lhs->val.path), point));
        } else if (agtv_lhs->type == DYNAMIC_CIRCLE) {
            agtv_result.val.circle = DatumGetCircleP(DirectFunctionCall2(circle_add_pt, CirclePGetDatum(agtv_lhs->val.circle), point));
        } else { 
            ereport_op_str("+", lhs, rhs);
        }
    } else if (agtv_lhs->type == DYNAMIC_INET && agtv_rhs->type == DYNAMIC_INTEGER)
    {
        agtv_result.type = DYNAMIC_INET;
        inet *i = DatumGetInetPP(DirectFunctionCall2(inetpl, InetPGetDatum(&agtv_lhs->val.inet),
				                             Int64GetDatum(agtv_rhs->val.int_value)));

        memcpy(&agtv_result.val.inet, i, sizeof(char) * 22);
    }
    else if (agtv_rhs->type == DYNAMIC_INET && agtv_lhs->type == DYNAMIC_INTEGER)
    {
        agtv_result.type = DYNAMIC_INET;
        inet *i = DatumGetInetPP(DirectFunctionCall2(inetpl, InetPGetDatum(&agtv_rhs->val.inet),
				                             Int64GetDatum(agtv_lhs->val.int_value)));

        memcpy(&agtv_result.val.inet, i, sizeof(char) * 22);
    } 
    else
	ereport_op_str("+", lhs, rhs);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&agtv_result));
}

PG_FUNCTION_INFO_V1(dynamic_sub);
// dynamic - dynamic operator
Datum dynamic_sub(PG_FUNCTION_ARGS)
{
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value *agtv_lhs;
    dynamic_value *agtv_rhs;
    dynamic_value agtv_result;

    if (!(DYNA_ROOT_IS_SCALAR(lhs)) || !(DYNA_ROOT_IS_SCALAR(rhs))) {

        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

    }

    agtv_lhs = get_ith_dynamic_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_dynamic_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INTEGER;
        agtv_result.val.int_value = GT_TO_INT8(lhs) - GT_TO_INT8(rhs);
    } else if ((agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_FLOAT) ||
               (agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_INTEGER) ||
               (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_FLOAT)) {
        agtv_result.type = DYNAMIC_FLOAT;
        agtv_result.val.float_value = GT_TO_FLOAT8(lhs) - GT_TO_FLOAT8(rhs);
    } else if (is_numeric_result(agtv_lhs, agtv_rhs)) {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_dynamic_value(agtv_lhs);
        rhsd = get_numeric_datum_from_dynamic_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_sub, lhsd, rhsd);

        agtv_result.type = DYNAMIC_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    } else if (agtv_rhs->type == DYNAMIC_INTERVAL) {
        Datum i = IntervalPGetDatum(&agtv_rhs->val.interval);
        agtv_result.type = agtv_lhs->type;

        if (agtv_lhs->type == DYNAMIC_TIMESTAMPTZ) {
            agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_mi_interval,
                                    TimestampTzGetDatum(agtv_lhs->val.int_value), i));
        } else if (agtv_lhs->type == DYNAMIC_TIMESTAMP) {
            agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(timestamp_mi_interval,
                                    TimestampGetDatum(agtv_lhs->val.int_value), i));
	} else if (agtv_lhs->type == DYNAMIC_DATE) {
             agtv_result.type = DYNAMIC_TIMESTAMPTZ;
             agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(date_mi_interval,
                                    DateADTGetDatum(agtv_lhs->val.date), i));
        }   else if (agtv_lhs->type == DYNAMIC_TIME) {
            agtv_result.val.int_value = DatumGetTimeADT(DirectFunctionCall2(time_mi_interval,
                                    TimeADTGetDatum(agtv_lhs->val.int_value), i));
        } else if (agtv_lhs->type == DYNAMIC_TIMETZ) {
            TimeTzADT *time = DatumGetTimeTzADTP(DirectFunctionCall2(timetz_mi_interval,
                                    TimeTzADTPGetDatum(&agtv_lhs->val.timetz), i));

            agtv_result.val.timetz.time = time->time;
            agtv_result.val.timetz.zone = time->zone;
        } else if (agtv_lhs->type == DYNAMIC_INTERVAL) {
            Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mi,
                                    IntervalPGetDatum(&agtv_lhs->val.interval), i));

            agtv_result.val.interval.time = interval->time;
            agtv_result.val.interval.day = interval->day;
            agtv_result.val.interval.month = interval->month;
        } else {
            ereport_op_str("-", lhs, rhs);
       }
    } else if (agtv_rhs->type == DYNAMIC_POINT) {
        Datum point = PointPGetDatum(agtv_rhs->val.point);
        agtv_result.type = agtv_lhs->type;
        if (agtv_lhs->type == DYNAMIC_POINT) {
            agtv_result.val.point = DatumGetPointP(DirectFunctionCall2(point_sub, PointPGetDatum(agtv_lhs->val.point), point));
        } else if (agtv_lhs->type == DYNAMIC_BOX) {
            agtv_result.val.box = DatumGetBoxP(DirectFunctionCall2(box_sub, BoxPGetDatum(agtv_lhs->val.box), point));
        } else if (agtv_lhs->type == DYNAMIC_PATH) {
            agtv_result.val.path = DatumGetPathP(DirectFunctionCall2(path_sub_pt, PathPGetDatum(agtv_lhs->val.path), point));
        } else if (agtv_lhs->type == DYNAMIC_CIRCLE) {
            agtv_result.val.circle = DatumGetCircleP(DirectFunctionCall2(circle_sub_pt, CirclePGetDatum(agtv_lhs->val.circle), point));
        } else {
            ereport_op_str("+", lhs, rhs);
        }
    } else if (agtv_lhs->type == DYNAMIC_INET && agtv_rhs->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INET;
        inet *i = DatumGetInetPP(DirectFunctionCall2(inetmi_int8, InetPGetDatum(&agtv_lhs->val.inet),
				                                  Int64GetDatum(agtv_rhs->val.int_value)));

        memcpy(&agtv_result.val.inet, i, sizeof(char) * 22);
    }
    else if (agtv_lhs->type == DYNAMIC_INET && agtv_rhs->type == DYNAMIC_INET)
    {
        agtv_result.type = DYNAMIC_INTEGER;
        agtv_result.val.int_value = DatumGetInt64(DirectFunctionCall2(inetmi, InetPGetDatum(&agtv_lhs->val.inet),
				                                              InetPGetDatum(&agtv_rhs->val.inet)));
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for dynamic_sub")));

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&agtv_result));
}

PG_FUNCTION_INFO_V1(dynamic_neg);
/*
 * dynamic negation function for unary - operator
 */
Datum dynamic_neg(PG_FUNCTION_ARGS)
{
    dynamic *v = AG_GET_ARG_DYNAMIC_P(0);
    dynamic_value *agtv_value;
    dynamic_value agtv_result;

    if (!(DYNA_ROOT_IS_SCALAR(v)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

    agtv_value = get_ith_dynamic_value_from_container(&v->root, 0);

    if (agtv_value->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INTEGER;
        agtv_result.val.int_value = -agtv_value->val.int_value;
    } else if (agtv_value->type == DYNAMIC_FLOAT) {
        agtv_result.type = DYNAMIC_FLOAT;
        agtv_result.val.float_value = -agtv_value->val.float_value;
    } else if (agtv_value->type == DYNAMIC_NUMERIC) {
        Datum numd, vald;

        vald = NumericGetDatum(agtv_value->val.numeric);
        numd = DirectFunctionCall1(numeric_uminus, vald);

        agtv_result.type = DYNAMIC_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    } else if (agtv_value->type == DYNAMIC_INTERVAL) {
        agtv_result.type = DYNAMIC_INTERVAL;
        Interval *interval =
	    DatumGetIntervalP(DirectFunctionCall1(interval_um, IntervalPGetDatum(&agtv_value->val.interval)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
	    
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter type for dynamic_neg")));
    }

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&agtv_result));
}

PG_FUNCTION_INFO_V1(dynamic_mul);
// dynamic * dynamic operator
Datum dynamic_mul(PG_FUNCTION_ARGS)
{
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value *agtv_lhs;
    dynamic_value *agtv_rhs;
    dynamic_value agtv_result;

    if (!(DYNA_ROOT_IS_SCALAR(lhs)) || !(DYNA_ROOT_IS_SCALAR(rhs))) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));
    }

    agtv_lhs = get_ith_dynamic_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_dynamic_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INTEGER;
        agtv_result.val.int_value = GT_TO_INT8(lhs) * GT_TO_INT8(rhs);
    } else if ((agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_FLOAT) ||
               (agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_INTEGER) ||
               (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_FLOAT)) {
        agtv_result.type = DYNAMIC_FLOAT;
        agtv_result.val.float_value = GT_TO_FLOAT8(lhs) * GT_TO_FLOAT8(rhs);
    } else if (is_numeric_result(agtv_lhs, agtv_rhs)) {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_dynamic_value(agtv_lhs);
        rhsd = get_numeric_datum_from_dynamic_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_mul, lhsd, rhsd);

        agtv_result.type = DYNAMIC_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    } else if (agtv_lhs->type == DYNAMIC_INTERVAL && agtv_rhs->type == DYNAMIC_FLOAT) {
        agtv_result.type = DYNAMIC_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mul,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                Float8GetDatum(agtv_rhs->val.float_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    } else if (agtv_rhs->type == DYNAMIC_POINT) {
        Datum point = PointPGetDatum(agtv_rhs->val.point);
        agtv_result.type = agtv_lhs->type;
        if (agtv_lhs->type == DYNAMIC_POINT) {
            agtv_result.val.point = DatumGetPointP(DirectFunctionCall2(point_mul, PointPGetDatum(agtv_lhs->val.point), point));
        } else if (agtv_lhs->type == DYNAMIC_BOX) {
            agtv_result.val.box = DatumGetBoxP(DirectFunctionCall2(box_mul, BoxPGetDatum(agtv_lhs->val.box), point));
        } else if (agtv_lhs->type == DYNAMIC_PATH) {
            agtv_result.val.path = DatumGetPathP(DirectFunctionCall2(path_mul_pt, PathPGetDatum(agtv_lhs->val.path), point));
        } else if (agtv_lhs->type == DYNAMIC_CIRCLE) {
            agtv_result.val.circle = DatumGetCircleP(DirectFunctionCall2(circle_mul_pt, CirclePGetDatum(agtv_lhs->val.circle), point));
        } else {
            ereport_op_str("*", lhs, rhs);
        }
    } else if (agtv_lhs->type == DYNAMIC_INTERVAL && agtv_rhs->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mul,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                Float8GetDatum((float8)agtv_rhs->val.int_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    } else if (agtv_rhs->type == DYNAMIC_INTERVAL && agtv_lhs->type == DYNAMIC_FLOAT) {
        agtv_result.type = DYNAMIC_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mul,
                                IntervalPGetDatum(&agtv_rhs->val.interval),
                                Float8GetDatum(agtv_lhs->val.float_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else if (agtv_rhs->type == DYNAMIC_INTERVAL && agtv_lhs->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mul,
                                IntervalPGetDatum(&agtv_rhs->val.interval),
                                Float8GetDatum((float8)agtv_lhs->val.int_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    } else
        ereport_op_str("*", lhs, rhs);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&agtv_result));
}

PG_FUNCTION_INFO_V1(dynamic_div);

/*
 * dynamic division function for / operator
 */
Datum dynamic_div(PG_FUNCTION_ARGS)
{
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value *agtv_lhs;
    dynamic_value *agtv_rhs;
    dynamic_value agtv_result;

    if (!(DYNA_ROOT_IS_SCALAR(lhs)) || !(DYNA_ROOT_IS_SCALAR(rhs)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

    agtv_lhs = get_ith_dynamic_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_dynamic_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_INTEGER) {
        if (agtv_rhs->val.int_value == 0)
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

        agtv_result.type = DYNAMIC_INTEGER;
        agtv_result.val.int_value = GT_TO_INT8(lhs) / GT_TO_INT8(rhs);
    } else if ((agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_FLOAT) ||
	       (agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_INTEGER) ||
               (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_FLOAT)) {
	float8 left = GT_TO_FLOAT8(lhs);
        float8 right = GT_TO_FLOAT8(rhs);

        if (right == 0)
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

        agtv_result.type = DYNAMIC_FLOAT;
        agtv_result.val.float_value = left / right;
    } else if (is_numeric_result(agtv_lhs, agtv_rhs)) {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_dynamic_value(agtv_lhs);
        rhsd = get_numeric_datum_from_dynamic_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_div, lhsd, rhsd);

        agtv_result.type = DYNAMIC_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    } else if (agtv_rhs->type == DYNAMIC_POINT) {
        Datum point = PointPGetDatum(agtv_rhs->val.point);
        agtv_result.type = agtv_lhs->type;
        if (agtv_lhs->type == DYNAMIC_POINT) {
            agtv_result.val.point = DatumGetPointP(DirectFunctionCall2(point_div, PointPGetDatum(agtv_lhs->val.point), point));
        } else if (agtv_lhs->type == DYNAMIC_BOX) {
            agtv_result.val.box = DatumGetBoxP(DirectFunctionCall2(box_div, BoxPGetDatum(agtv_lhs->val.box), point));
        } else if (agtv_lhs->type == DYNAMIC_PATH) {
            agtv_result.val.path = DatumGetPathP(DirectFunctionCall2(path_div_pt, PathPGetDatum(agtv_lhs->val.path), point));
        } else if (agtv_lhs->type == DYNAMIC_CIRCLE) {
            agtv_result.val.circle = DatumGetCircleP(DirectFunctionCall2(circle_div_pt, CirclePGetDatum(agtv_lhs->val.circle), point));
        } else {
            ereport_op_str("/", lhs, rhs);
        }
    } else if (agtv_lhs->type == DYNAMIC_INTERVAL && agtv_rhs->type == DYNAMIC_FLOAT) {
        agtv_result.type = DYNAMIC_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_div,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                Float8GetDatum(agtv_rhs->val.float_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    } else if (agtv_lhs->type == DYNAMIC_INTERVAL && agtv_rhs->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_div,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                Float8GetDatum((float8)agtv_rhs->val.int_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else
        ereport_op_str("/", lhs, rhs);

     AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&agtv_result));
}

PG_FUNCTION_INFO_V1(dynamic_mod);
/*
 * dynamic modulo function for % operator
 */
Datum dynamic_mod(PG_FUNCTION_ARGS)
{
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value *agtv_lhs;
    dynamic_value *agtv_rhs;
    dynamic_value agtv_result;

    if (!(DYNA_ROOT_IS_SCALAR(lhs)) || !(DYNA_ROOT_IS_SCALAR(rhs)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("must be scalar value, not array or object")));

    agtv_lhs = get_ith_dynamic_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_dynamic_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_INTEGER) {
        agtv_result.type = DYNAMIC_INTEGER;
        agtv_result.val.int_value = GT_TO_INT8(lhs) % GT_TO_INT8(rhs);
    } else if ((agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_FLOAT) ||
               (agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_INTEGER) ||
               (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_FLOAT)) {
        agtv_result.type = DYNAMIC_FLOAT;
        agtv_result.val.float_value = fmod(GT_TO_FLOAT8(lhs), GT_TO_FLOAT8(rhs));
    }
    /* Is this a numeric result */
    else if (is_numeric_result(agtv_lhs, agtv_rhs))
    {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_dynamic_value(agtv_lhs);
        rhsd = get_numeric_datum_from_dynamic_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_mod, lhsd, rhsd);

        agtv_result.type = DYNAMIC_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for dynamic_mod")));

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&agtv_result));
}

PG_FUNCTION_INFO_V1(dynamic_pow);

/*
 * dynamic power function for ^ operator
 */
Datum dynamic_pow(PG_FUNCTION_ARGS)
{
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value *agtv_lhs;
    dynamic_value *agtv_rhs;
    dynamic_value agtv_result;

    if (!(DYNA_ROOT_IS_SCALAR(lhs)) || !(DYNA_ROOT_IS_SCALAR(rhs)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

    agtv_lhs = get_ith_dynamic_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_dynamic_value_from_container(&rhs->root, 0);

    if ((agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_INTEGER) ||
        (agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_FLOAT) ||
        (agtv_lhs->type == DYNAMIC_FLOAT && agtv_rhs->type == DYNAMIC_INTEGER) ||
        (agtv_lhs->type == DYNAMIC_INTEGER && agtv_rhs->type == DYNAMIC_FLOAT)) {
        agtv_result.type = DYNAMIC_FLOAT;
        agtv_result.val.float_value = pow(GT_TO_FLOAT8(lhs), GT_TO_FLOAT8(rhs));
    } else if (is_numeric_result(agtv_lhs, agtv_rhs)) {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_dynamic_value(agtv_lhs);
        rhsd = get_numeric_datum_from_dynamic_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_power, lhsd, rhsd);

        agtv_result.type = DYNAMIC_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for dynamic_pow")));
    }
 
    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&agtv_result));
}

PG_FUNCTION_INFO_V1(dynamic_bitwise_not);
Datum
dynamic_bitwise_not(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(inetnot, GT_ARG_TO_INET_DATUM(0));

    dynamic_value gtv;
    gtv.type = DYNAMIC_INET;
    memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_bitwise_and);
Datum
dynamic_bitwise_and(PG_FUNCTION_ARGS) {
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);

    if (DYNA_IS_TSQUERY(lhs) || DYNA_IS_TSQUERY(rhs)) {
        Datum d = DirectFunctionCall2(tsquery_and, GT_ARG_TO_TSQUERY_DATUM(0), GT_ARG_TO_TSQUERY_DATUM(1));


        dynamic_value gtv;
        gtv.type = DYNAMIC_TSQUERY;
        gtv.val.tsquery = DatumGetPointer(d); 

    	AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
    } else {
        Datum d = DirectFunctionCall2(inetand, GT_ARG_TO_INET_DATUM(0), GT_ARG_TO_INET_DATUM(1));

        dynamic_value gtv;
        gtv.type = DYNAMIC_INET;
        memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);
   
       	AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
    }
}


PG_FUNCTION_INFO_V1(dynamic_bitwise_or);
Datum
dynamic_bitwise_or(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall2(inetor, GT_ARG_TO_INET_DATUM(0), GT_ARG_TO_INET_DATUM(1));

    dynamic_value gtv;
    gtv.type = DYNAMIC_INET;
    memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}


PG_FUNCTION_INFO_V1(dynamic_inet_subnet_strict_contains);
Datum
dynamic_inet_subnet_strict_contains(PG_FUNCTION_ARGS) {
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);

    if (DYNA_IS_BOX(lhs) && DYNA_IS_BOX(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(box_left, GT_TO_BOX_DATUM(lhs), GT_TO_BOX_DATUM(rhs))));
    else if (DYNA_IS_POLYGON(lhs) && DYNA_IS_POLYGON(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(poly_left, GT_TO_POLYGON_DATUM(lhs), GT_TO_POLYGON_DATUM(rhs))));
    else if (DYNA_IS_CIRCLE(lhs) && DYNA_IS_CIRCLE(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(circle_left, GT_TO_CIRCLE_DATUM(lhs), GT_TO_CIRCLE_DATUM(rhs))));
    
    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(network_sub, GT_ARG_TO_INET_DATUM(0), GT_ARG_TO_INET_DATUM(1))));
}

PG_FUNCTION_INFO_V1(dynamic_inet_subnet_contains);
Datum
dynamic_inet_subnet_contains(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(network_subeq, GT_ARG_TO_INET_DATUM(0), GT_ARG_TO_INET_DATUM(1))));
}


PG_FUNCTION_INFO_V1(dynamic_inet_subnet_strict_contained_by);
Datum
dynamic_inet_subnet_strict_contained_by(PG_FUNCTION_ARGS) {
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);

    if (DYNA_IS_BOX(lhs) && DYNA_IS_BOX(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(box_right, GT_TO_BOX_DATUM(lhs), GT_TO_BOX_DATUM(rhs))));
    else if (DYNA_IS_POLYGON(lhs) && DYNA_IS_POLYGON(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(poly_right, GT_TO_POLYGON_DATUM(lhs), GT_TO_POLYGON_DATUM(rhs))));
    else if (DYNA_IS_CIRCLE(lhs) && DYNA_IS_CIRCLE(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(circle_right, GT_TO_CIRCLE_DATUM(lhs), GT_TO_CIRCLE_DATUM(rhs))));

    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(network_sup, GT_ARG_TO_INET_DATUM(0), GT_ARG_TO_INET_DATUM(1))));
}

PG_FUNCTION_INFO_V1(dynamic_inet_subnet_contained_by);
Datum
dynamic_inet_subnet_contained_by(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(network_supeq, GT_ARG_TO_INET_DATUM(0), GT_ARG_TO_INET_DATUM(1))));
}


PG_FUNCTION_INFO_V1(dynamic_inet_subnet_contain_both);
Datum
dynamic_inet_subnet_contain_both(PG_FUNCTION_ARGS) {
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);

    if (DYNA_IS_BOX(lhs) && DYNA_IS_BOX(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(box_overlap, GT_TO_BOX_DATUM(lhs), GT_TO_BOX_DATUM(rhs))));
    else if (DYNA_IS_POLYGON(lhs) && DYNA_IS_POLYGON(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(poly_overlap, GT_TO_POLYGON_DATUM(lhs), GT_TO_POLYGON_DATUM(rhs))));
    else if (DYNA_IS_CIRCLE(lhs) && DYNA_IS_CIRCLE(rhs))
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(circle_overlap, GT_TO_CIRCLE_DATUM(lhs), GT_TO_CIRCLE_DATUM(rhs))));

    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(network_overlap, GT_TO_INET_DATUM(lhs), GT_TO_INET_DATUM(rhs))));
}

#define DYNAMICCMPFUNC( type, action)                                                        \
PG_FUNCTION_INFO_V1(dynamic_##type);                                                         \
Datum                                                                                      \
dynamic_##type(PG_FUNCTION_ARGS)                                                             \
{                                                                                          \
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);                                                    \
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);                                                    \
    int result = (compare_dynamic_containers_orderability(&lhs->root, &rhs->root) action 0); \
    PG_FREE_IF_COPY(lhs,0);                                                                \
    PG_FREE_IF_COPY(rhs,1);                                                                \
    PG_RETURN_BOOL( result );                                                              \
}                                                                                          \
/* keep compiler quiet - no extra ; */                                                     \
extern int no_such_variable

DYNAMICCMPFUNC(lt, <);
DYNAMICCMPFUNC(le, <=);
DYNAMICCMPFUNC(eq, ==);
DYNAMICCMPFUNC(ge, >=);
DYNAMICCMPFUNC(gt, >);
DYNAMICCMPFUNC(ne, !=);

PG_FUNCTION_INFO_V1(dynamic_contains);
/*
 * <@ operator for dynamic. Returns true if the right dynamic path/value entries
 * contained at the top level within the left dynamic value
 */
Datum dynamic_contains(PG_FUNCTION_ARGS) {
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);

    if (DYNA_IS_TSQUERY(lhs) || DYNA_IS_TSQUERY(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(tsq_mcontains,
                                                        GT_TO_TSQUERY_DATUM(lhs),
                                                        GT_TO_TSQUERY_DATUM(rhs)));

	PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_BOX(lhs) && DYNA_IS_POINT(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(box_contain_pt,
                                                        GT_TO_BOX_DATUM(lhs),
                                                        GT_TO_POINT_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_BOX(lhs) && DYNA_IS_BOX(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(box_contain,
                                                        GT_TO_BOX_DATUM(lhs),
                                                        GT_TO_BOX_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_PATH(lhs) && DYNA_IS_POINT(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(on_ppath, GT_TO_POINT_DATUM(rhs), GT_TO_PATH_DATUM(lhs)));
        PG_RETURN_BOOL(boolean);
     }else if (DYNA_IS_POLYGON(lhs) && DYNA_IS_POINT(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(poly_contain_pt, GT_TO_POLYGON_DATUM(lhs), GT_TO_POINT_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_CIRCLE(lhs) && DYNA_IS_POINT(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(circle_contain_pt, GT_TO_CIRCLE_DATUM(lhs), GT_TO_POINT_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_CIRCLE(lhs) && DYNA_IS_CIRCLE(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(circle_contained, GT_TO_CIRCLE_DATUM(lhs), GT_TO_CIRCLE_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    }

    dynamic_iterator *constraint_it = dynamic_iterator_init(&(rhs->root));
    dynamic_iterator *property_it = dynamic_iterator_init(&(lhs->root));

    PG_RETURN_BOOL(dynamic_deep_contains(&property_it, &constraint_it));
}


PG_FUNCTION_INFO_V1(dynamic_contained_by);
/*
 * <@ operator for dynamic. Returns true if the left dynamic path/value entries
 * contained at the top level within the right dynamic value
 */
Datum dynamic_contained_by(PG_FUNCTION_ARGS) {
    dynamic *lhs = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *rhs = AG_GET_ARG_DYNAMIC_P(1);

    if (DYNA_IS_TSQUERY(lhs) || DYNA_IS_TSQUERY(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(tsq_mcontains,
                                                        GT_TO_TSQUERY_DATUM(rhs),
                                                        GT_TO_TSQUERY_DATUM(lhs)));

        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_BOX(rhs) && DYNA_IS_POINT(lhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(on_pb,
                                                        GT_TO_POINT_DATUM(lhs),
                                                        GT_TO_BOX_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_BOX(lhs) && DYNA_IS_BOX(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(box_contained,
                                                        GT_TO_BOX_DATUM(lhs),
                                                        GT_TO_BOX_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_PATH(rhs) && DYNA_IS_POINT(lhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(on_ppath, GT_TO_POINT_DATUM(lhs), GT_TO_PATH_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    }  else if (DYNA_IS_POLYGON(rhs) && DYNA_IS_POINT(lhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(pt_contained_poly, GT_TO_POINT_DATUM(lhs), GT_TO_POLYGON_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_CIRCLE(rhs) && DYNA_IS_POINT(lhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(pt_contained_circle, GT_TO_POINT_DATUM(lhs), GT_TO_CIRCLE_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    } else if (DYNA_IS_CIRCLE(lhs) && DYNA_IS_CIRCLE(rhs)) {
        bool boolean = DatumGetBool(DirectFunctionCall2(circle_contain, GT_TO_CIRCLE_DATUM(lhs), GT_TO_CIRCLE_DATUM(rhs)));
        PG_RETURN_BOOL(boolean);
    }



    dynamic_iterator *constraint_it = dynamic_iterator_init(&(rhs->root));
    dynamic_iterator *property_it = dynamic_iterator_init(&(lhs->root));

    PG_RETURN_BOOL(dynamic_deep_contains(&constraint_it, &property_it));
}

PG_FUNCTION_INFO_V1(dynamic_exists);
/*
 * ? operator for dynamic. Returns true if the string exists as top-level keys
 */
Datum dynamic_exists(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *key = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value aval;
    dynamic_value *v = NULL;

    /*
     * We only match Object keys (which are naturally always Strings), or
     * string elements in arrays.  In particular, we do not match non-string
     * scalar elements.  Existence of a key/element is only considered at the
     * top level.  No recursion occurs.
     */
    aval.type = DYNAMIC_STRING;
    aval.val.string.val = VARDATA_ANY(key);
    aval.val.string.len = VARSIZE_ANY_EXHDR(key);
    if (!(DYNA_ROOT_IS_SCALAR(key)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dynamic ? dynamic arg 2 must be a string")));

    v = get_ith_dynamic_value_from_container(&key->root, 0);

    if (v->type != DYNAMIC_STRING)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dynamic ? dynamic arg 2 must be a string")));

    v = find_dynamic_value_from_container(&agt->root, GT_FOBJECT | GT_FARRAY, v);

    PG_RETURN_BOOL(v != NULL);
}

PG_FUNCTION_INFO_V1(dynamic_exists_any);
/*
 * ?| operator for dynamic. Returns true if any of the array strings exist as
 * top-level keys
 */
Datum dynamic_exists_any(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *keys = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value agtv_elem;

    if (DYNA_IS_POINT(agt) || DYNA_IS_POINT(agt)) {
        Datum d = DirectFunctionCall2(point_vert, GT_TO_POINT_DATUM(agt), GT_TO_POINT_DATUM(keys));

        PG_RETURN_BOOL(DatumGetBool(d));
    }

    if (!DYNA_ROOT_IS_ARRAY(keys) || DYNA_ROOT_IS_SCALAR(keys))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("dynamic ?| dynamic rhs must be a list of strings")));

    dynamic_iterator *it_array = dynamic_iterator_init(&keys->root);
    dynamic_iterator_next(&it_array, &agtv_elem, false);

    while (dynamic_iterator_next(&it_array, &agtv_elem, false) != WGT_END_ARRAY)
    {           

	if (agtv_elem.type == DYNAMIC_NULL)
            continue;

        if (agtv_elem.type != DYNAMIC_STRING)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("All keys of ?| Operator must be strings")));
        
        if (find_dynamic_value_from_container(&agt->root, GT_FOBJECT | GT_FARRAY, &agtv_elem) != NULL)
            PG_RETURN_BOOL(true);
    }
    
    PG_RETURN_BOOL(false);
}

PG_FUNCTION_INFO_V1(dynamic_exists_all);
/*
 * ?& operator for dynamic. Returns true if all of the array strings exist as
 * top-level keys
 */
Datum dynamic_exists_all(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    dynamic *keys = AG_GET_ARG_DYNAMIC_P(1);
    dynamic_value agtv_elem;

    if (!DYNA_ROOT_IS_ARRAY(keys) || DYNA_ROOT_IS_SCALAR(keys))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("dynamic ?& dynamic rhs must be a list of strings")));

    dynamic_iterator *it_array = dynamic_iterator_init(&keys->root);
    dynamic_iterator_next(&it_array, &agtv_elem, false);

    while (dynamic_iterator_next(&it_array, &agtv_elem, false) != WGT_END_ARRAY)
    {

        if (agtv_elem.type == DYNAMIC_NULL)
            continue;

        if (agtv_elem.type != DYNAMIC_STRING)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("All keys of ?& Operator must be strings")));

        if (find_dynamic_value_from_container(&agt->root, GT_FOBJECT | GT_FARRAY, &agtv_elem) == NULL)
            PG_RETURN_BOOL(false);
    }
  
    PG_RETURN_BOOL(true);
}

static dynamic *dynamic_concat(dynamic *agt1, dynamic *agt2)
{
    dynamic_parse_state *state = NULL;
    dynamic_value *res;
    dynamic_iterator *it1;
    dynamic_iterator *it2;

    /*
     * If one of the dynamic is empty, just return the other if it's not scalar
     * and both are of the same kind.  If it's a scalar or they are of
     * different kinds we need to perform the concatenation even if one is
     * empty.
     */
    if (DYNA_ROOT_IS_OBJECT(agt1) && DYNA_ROOT_IS_OBJECT(agt2)) {
        if (DYNA_ROOT_COUNT(agt1) == 0 && !DYNA_ROOT_IS_SCALAR(agt2))
            return agt2;
        else if (DYNA_ROOT_COUNT(agt2) == 0 && !DYNA_ROOT_IS_SCALAR(agt1))
            return agt1;
    }

    it1 = dynamic_iterator_init(&agt1->root);
    it2 = dynamic_iterator_init(&agt2->root);

    res = iterator_concat(&it1, &it2, &state);

    Assert(res != NULL);

    return (dynamic_value_to_dynamic(res));
}

/*
 * Iterate over all dynamic objects and merge them into one.
 * The logic of this function copied from the same hstore function,
 * except the case, when it1 & it2 represents jbvObject.
 * In that case we just append the content of it2 to it1 without any
 * verifications.
 */
static dynamic_value *iterator_concat(dynamic_iterator **it1, dynamic_iterator **it2, dynamic_parse_state **state) {
    dynamic_value v1, v2, *res = NULL;
    dynamic_iterator_token r1, r2, rk1, rk2;

    r1 = rk1 = dynamic_iterator_next(it1, &v1, false);
    r2 = rk2 = dynamic_iterator_next(it2, &v2, false);

    /*
     * Both elements are objects.
     */
    if (rk1 == WGT_BEGIN_OBJECT && rk2 == WGT_BEGIN_OBJECT) {
        /*
         * Append the all tokens from v1 to res, except last WGT_END_OBJECT
         * (because res will not be finished yet).
         */
        push_dynamic_value(state, r1, NULL);
        while ((r1 = dynamic_iterator_next(it1, &v1, true)) != WGT_END_OBJECT)
            push_dynamic_value(state, r1, &v1);

        /*
         * Append the all tokens from v2 to res, include last WGT_END_OBJECT
         * (the concatenation will be completed).
         */
        while ((r2 = dynamic_iterator_next(it2, &v2, true)) != 0)
            res = push_dynamic_value(state, r2, r2 != WGT_END_OBJECT ? &v2 : NULL);
    }
    /*
     * Both elements are arrays (either can be scalar).
     */
    else if (rk1 == WGT_BEGIN_ARRAY && rk2 == WGT_BEGIN_ARRAY) {
        push_dynamic_value(state, r1, NULL);

        while ((r1 = dynamic_iterator_next(it1, &v1, true)) != WGT_END_ARRAY) {
            Assert(r1 == WGT_ELEM);
            push_dynamic_value(state, r1, &v1);
        }

        while ((r2 = dynamic_iterator_next(it2, &v2, true)) != WGT_END_ARRAY) {
            Assert(r2 == WGT_ELEM);
            push_dynamic_value(state, WGT_ELEM, &v2);
        }

        res = push_dynamic_value(state, WGT_END_ARRAY, NULL);
    }
    /* have we got array || object or object || array? */
    else if (((rk1 == WGT_BEGIN_ARRAY && !(*it1)->is_scalar) && rk2 == WGT_BEGIN_OBJECT) ||
             (rk1 == WGT_BEGIN_OBJECT && (rk2 == WGT_BEGIN_ARRAY && !(*it2)->is_scalar))) {
        dynamic_iterator **it_array = rk1 == WGT_BEGIN_ARRAY ? it1 : it2;
        dynamic_iterator **it_object = rk1 == WGT_BEGIN_OBJECT ? it1 : it2;

        bool prepend = (rk1 == WGT_BEGIN_OBJECT);

        push_dynamic_value(state, WGT_BEGIN_ARRAY, NULL);

        if (prepend) {
            push_dynamic_value(state, WGT_BEGIN_OBJECT, NULL);
            while ((r1 = dynamic_iterator_next(it_object, &v1, true)) != 0)
                push_dynamic_value(state, r1, r1 != WGT_END_OBJECT ? &v1 : NULL);

            while ((r2 = dynamic_iterator_next(it_array, &v2, true)) != 0)
                res = push_dynamic_value(state, r2, r2 != WGT_END_ARRAY ? &v2 : NULL);
        } else {
            while ((r1 = dynamic_iterator_next(it_array, &v1, true)) != WGT_END_ARRAY)
                push_dynamic_value(state, r1, &v1);

            push_dynamic_value(state, WGT_BEGIN_OBJECT, NULL);
            while ((r2 = dynamic_iterator_next(it_object, &v2, true)) != 0)
                push_dynamic_value(state, r2, r2 != WGT_END_OBJECT ? &v2 : NULL);

            res = push_dynamic_value(state, WGT_END_ARRAY, NULL);
        }
    } else {
        /*
         * This must be scalar || object or object || scalar, as that's all
         * that's left. Both of these make no sense, so error out.
         */
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid concatenation of dynamic objects")));
    }

    return res;
}

static void ereport_op_str(const char *op, dynamic *lhs, dynamic *rhs) {
    const char *msgfmt;
    const char *lstr;
    const char *rstr;

    Assert(rhs != NULL);

    if (lhs == NULL) {
        msgfmt = "invalid expression: %s%s%s";
        lstr = "";
    } else {
        msgfmt = "invalid expression: %s %s %s";
        lstr = dynamic_to_cstring(NULL, &lhs->root, VARSIZE(lhs));
    }
    rstr = dynamic_to_cstring(NULL, &rhs->root, VARSIZE(rhs));

    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg(msgfmt, lstr, op, rstr)));
}
