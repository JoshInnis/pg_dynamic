#include "utils/timestamp.h"

typedef Datum (*coearce_function) (dynamic_value *);
Datum convert_to_scalar(coearce_function func, dynamic *agt, char *type);

#define GT_TO_INT8(arg) \
    DatumGetInt64(GT_TO_INT8_DATUM(arg))
#define GT_TO_FLOAT8(arg) \
    DatumGetFloat8(GT_TO_FLOAT8_DATUM(arg))
#define GT_TO_NUMERIC(arg) \
    DatumGetFloat8(GT_TO_NUMERIC_DATUM(arg))
#define GT_TO_TEXT(arg) \
    DatumGetTextP(GT_TO_TEXT_DATUM(arg))
#define GT_TO_STRING(arg) \
    DatumGetCString(GT_TO_STRING_DATUM(arg))


#define GT_TO_INT4_DATUM(arg) \
    convert_to_scalar(dynamic_to_int4_internal, arg, "int4")
#define GT_TO_INT8_DATUM(arg) \
    convert_to_scalar(dynamic_to_int8_internal, arg, "int")
#define GT_TO_FLOAT8_DATUM(arg) \
    convert_to_scalar(dynamic_to_float8_internal, arg, "float")
#define GT_TO_NUMERIC_DATUM(arg) \
    convert_to_scalar(dynamic_to_numeric_internal, arg, "numeric")
#define GT_TO_TEXT_DATUM(arg) \
    convert_to_scalar(dynamic_to_text_internal, (arg), "text")
#define GT_TO_STRING_DATUM(arg) \
    convert_to_scalar(dynamic_to_string_internal, (arg), "string")
#define GT_TO_TIMESTAMP_DATUM(arg) \
    convert_to_scalar(dynamic_to_timestamp_internal, (arg), "timestamp")
#define GT_TO_TIMESTAMPTZ_DATUM(arg) \
    convert_to_scalar(dynamic_to_timestamptz_internal, (arg), "timestamptz")
#define GT_TO_DATE_DATUM(arg) \
    convert_to_scalar(dynamic_to_date_internal, (arg), "date")
#define GT_TO_TIME_DATUM(arg) \
    convert_to_scalar(dynamic_to_time_internal, (arg), "time")
#define GT_TO_TIMETZ_DATUM(arg) \
    convert_to_scalar(dynamic_to_timetz_internal, (arg), "time")
#define GT_TO_INTERVAL_DATUM(arg) \
    convert_to_scalar(dynamic_to_interval_internal, (arg), "interval")
#define GT_TO_CIDR_DATUM(arg) \
    convert_to_scalar(dynamic_to_cidr_internal, (arg), "cidr")
#define GT_TO_INET_DATUM(arg) \
    convert_to_scalar(dynamic_to_inet_internal, (arg), "inet")
#define GT_TO_MAC_DATUM(arg) \
    convert_to_scalar(dynamic_to_macaddr_internal, (arg), "mac")
#define GT_TO_MAC8_DATUM(arg) \
    convert_to_scalar(dynamic_to_macaddr8_internal, (arg), "mac8")
#define GT_TO_POINT_DATUM(arg) \
    convert_to_scalar(dynamic_to_point_internal, (arg), "point")
#define GT_TO_PATH_DATUM(arg) \
    convert_to_scalar(dynamic_to_path_internal, (arg), "path")
#define GT_TO_POLYGON_DATUM(arg) \
    convert_to_scalar(dynamic_to_polygon_internal, (arg), "polygon")
#define GT_TO_CIRCLE_DATUM(arg) \
    convert_to_scalar(dynamic_to_circle_internal, (arg), "circle")
#define GT_TO_LSEG_DATUM(arg) \
    convert_to_scalar(dynamic_to_lseg_internal, (arg), "lseg")
#define GT_TO_LINE_DATUM(arg) \
    convert_to_scalar(dynamic_to_line_internal, (arg), "line")
#define GT_TO_BOX_DATUM(arg) \
    convert_to_scalar(dynamic_to_box_internal, (arg), "box")
#define GT_TO_TSVECTOR_DATUM(arg) \
    convert_to_scalar(dynamic_to_tsvector_internal, (arg), "tsvector")
#define GT_TO_TSQUERY_DATUM(arg) \
    convert_to_scalar(dynamic_to_tsquery_internal, (arg), "tsquery")


#define GT_ARG_TO_INT4_DATUM(arg) \
    convert_to_scalar(dynamic_to_int4_internal, AG_GET_ARG_DYNAMIC_P(arg), "int4")
#define GT_ARG_TO_INT8_DATUM(arg) \
    convert_to_scalar(dynamic_to_int8_internal, AG_GET_ARG_DYNAMIC_P(arg), "int")
#define GT_ARG_TO_FLOAT8_DATUM(arg) \
    convert_to_scalar(dynamic_to_float8_internal, AG_GET_ARG_DYNAMIC_P(arg), "float")
#define GT_ARG_TO_NUMERIC_DATUM(arg) \
    convert_to_scalar(dynamic_to_numeric_internal, AG_GET_ARG_DYNAMIC_P(arg), "numeric")
#define GT_ARG_TO_TEXT_DATUM(arg) \
    convert_to_scalar(dynamic_to_text_internal, AG_GET_ARG_DYNAMIC_P(arg), "text")
#define GT_ARG_TO_STRING_DATUM(arg) \
    convert_to_scalar(dynamic_to_string_internal, AG_GET_ARG_DYNAMIC_P(arg), "string")
#define GT_ARG_TO_TIMESTAMP_DATUM(arg) \
    convert_to_scalar(dynamic_to_timestamp_internal, AG_GET_ARG_DYNAMIC_P(arg), "timestamp")
#define GT_ARG_TO_TIMESTAMPTZ_DATUM(arg) \
    convert_to_scalar(dynamic_to_timestamptz_internal, AG_GET_ARG_DYNAMIC_P(arg), "timestamptz")
#define GT_ARG_TO_DATE_DATUM(arg) \
    convert_to_scalar(dynamic_to_date_internal, AG_GET_ARG_DYNAMIC_P(arg), "date")
#define GT_ARG_TO_INET_DATUM(arg) \
    convert_to_scalar(dynamic_to_inet_internal, AG_GET_ARG_DYNAMIC_P(arg), "inet")
#define GT_ARG_TO_MAC8_DATUM(arg) \
    convert_to_scalar(dynamic_to_macaddr8_internal, AG_GET_ARG_DYNAMIC_P(arg), "mac8")
#define GT_ARG_TO_POINT_DATUM(arg) \
    convert_to_scalar(dynamic_to_point_internal, AG_GET_ARG_DYNAMIC_P(arg), "point")
#define GT_ARG_TO_CIRCLE_DATUM(arg) \
    convert_to_scalar(dynamic_to_circle_internal, AG_GET_ARG_DYNAMIC_P(arg), "circle")
#define GT_ARG_TO_LSEG_DATUM(arg) \
    convert_to_scalar(dynamic_to_lseg_internal, AG_GET_ARG_DYNAMIC_P(arg), "lseg")
#define GT_ARG_TO_LINE_DATUM(arg) \
    convert_to_scalar(dynamic_to_line_internal, AG_GET_ARG_DYNAMIC_P(arg), "line")
#define GT_ARG_TO_TSQUERY_DATUM(arg) \
    convert_to_scalar(dynamic_to_tsquery_internal, AG_GET_ARG_DYNAMIC_P(arg), "tsquery")


Datum dynamic_to_int8_internal(dynamic_value *gtv);
Datum dynamic_to_int4_internal(dynamic_value *gtv);
Datum dynamic_to_int2_internal(dynamic_value *gtv);
Datum dynamic_to_float8_internal(dynamic_value *gtv);
Datum dynamic_to_float4_internal(dynamic_value *gtv);
Datum dynamic_to_numeric_internal(dynamic_value *gtv);
Datum dynamic_to_string_internal(dynamic_value *gtv);
Datum dynamic_to_text_internal(dynamic_value *gtv);
Datum dynamic_to_boolean_internal(dynamic_value *gtv);
Datum dynamic_to_date_internal(dynamic_value *gtv);
Datum dynamic_to_time_internal(dynamic_value *gtv);
Datum dynamic_to_timetz_internal(dynamic_value *gtv);
Datum dynamic_to_timestamptz_internal(dynamic_value *gtv);
Datum dynamic_to_timestamp_internal(dynamic_value *gtv);
Datum dynamic_to_interval_internal(dynamic_value *gtv);
Datum dynamic_to_inet_internal(dynamic_value *gtv);
Datum dynamic_to_cidr_internal(dynamic_value *gtv);
Datum dynamic_to_macaddr_internal(dynamic_value *gtv);
Datum dynamic_to_macaddr8_internal(dynamic_value *gtv);
Datum dynamic_to_point_internal(dynamic_value *gtv);
Datum dynamic_to_path_internal(dynamic_value *gtv);
Datum dynamic_to_polygon_internal(dynamic_value *gtv);
Datum dynamic_to_circle_internal(dynamic_value *gtv) ;
Datum dynamic_to_box_internal(dynamic_value *gtv);
Datum dynamic_to_int_range_internal(dynamic_value *gtv);
Datum dynamic_to_tsvector_internal(dynamic_value *gtv);
Datum dynamic_to_tsquery_internal(dynamic_value *gtv);
Datum dynamic_to_lseg_internal(dynamic_value *gtv);
Datum dynamic_to_line_internal(dynamic_value *gtv);

Datum _dynamic_toinet(Datum arg);

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
#include "utils/dynamic_typecasting.h"

#define int8_to_int4 int84
#define int8_to_int2 int82
#define int8_to_numeric int8_numeric
#define int8_to_string int8out

#define float8_to_int8 dtoi8
#define float8_to_int4 dtoi4
#define float8_to_int2 dtoi2
#define float8_to_float4 dtof
#define float8_to_numeric float8_numeric
#define float8_to_string float8out

#define numeric_to_int8 numeric_int8
#define numeric_to_int4 numeric_int4
#define numeric_to_int2 numeric_int2
#define numeric_to_float4 numeric_float4
#define numeric_to_string numeric_out

#define string_to_int8 int8in
#define string_to_int4 int4in
#define string_to_int2 int2in
#define string_to_numeric numeric_in

static ArrayType *dynamic_to_array(coearce_function func, dynamic *agt, char *type, Oid type_oid, int type_len, bool elembyval); 


static void cannot_cast_dynamic_value(enum dynamic_value_type type, const char *sqltype);

Datum convert_to_scalar(coearce_function func, dynamic *agt, char *type) {
    if (!DYNA_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot cast non-scalar dynamic to %s", type)));

    dynamic_value *gtv = get_ith_dynamic_value_from_container(&agt->root, 0);

    Datum d = func(gtv);

    return d;
}

Datum boolean_to_dynamic(bool b) {
    dynamic_value gtv;
    dynamic *agt;

    gtv.type = DYNAMIC_BOOL;
    gtv.val.boolean = b;
    agt = dynamic_value_to_dynamic(&gtv);

    return DYNAMIC_P_GET_DATUM(agt);
}

PG_FUNCTION_INFO_V1(dynamic_tointeger);
Datum
dynamic_tointeger(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    dynamic_value gtv = {
        .type = DYNAMIC_INTEGER,
        .val.int_value = DatumGetInt64(convert_to_scalar(dynamic_to_int8_internal, agt, "dynamic integer"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tofloat);
Datum
dynamic_tofloat(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    dynamic_value gtv = {
        .type = DYNAMIC_FLOAT,
        .val.float_value = DatumGetFloat8(convert_to_scalar(dynamic_to_float8_internal, agt, "dynamic float"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}


PG_FUNCTION_INFO_V1(dynamic_tonumeric);
// dynamic -> dynamic numeric
Datum
dynamic_tonumeric(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();


    dynamic_value gtv = {
        .type = DYNAMIC_NUMERIC,
        .val.numeric = DatumGetNumeric(convert_to_scalar(dynamic_to_numeric_internal, agt, "dynamic numeric"))
    };

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tostring);
Datum
dynamic_tostring(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    char *string = DatumGetCString(convert_to_scalar(dynamic_to_string_internal, agt, "string"));
    
    dynamic_value gtv; 
    gtv.type = DYNAMIC_STRING;
    gtv.val.string.val = string;
    gtv.val.string.len = strlen(string);

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}


PG_FUNCTION_INFO_V1(dynamic_tobytea);
Datum dynamic_tobytea(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    bytea *bytea = DatumGetPointer(DirectFunctionCall1(byteain,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string")));

    dynamic_value gtv;
    gtv.type = DYNAMIC_BYTEA;
    gtv.val.bytea = bytea;
    
    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_toboolean);
Datum
dynamic_toboolean(PG_FUNCTION_ARGS) {

    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_boolean_internal, agt, "bool");

    PG_FREE_IF_COPY(agt, 0);

    dynamic_value gtv = { .type = DYNAMIC_BOOL, .val.boolean = BoolGetDatum(d) };

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_totimestamp);
Datum
dynamic_totimestamp(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    int64 ts = DatumGetInt64(convert_to_scalar(dynamic_to_timestamp_internal, agt, "timestamp"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_TIMESTAMP;
    gtv.val.int_value = ts;

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(totimestamptz);            
Datum totimestamptz(PG_FUNCTION_ARGS)
{               
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    int64 ts = DatumGetInt64(convert_to_scalar(dynamic_to_timestamptz_internal, agt, "timestamptz"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_TIMESTAMPTZ;
    gtv.val.int_value = ts;

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}           

PG_FUNCTION_INFO_V1(totime);              
Datum totime(PG_FUNCTION_ARGS)
{  
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    Timestamp t;

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    t = DatumGetInt64(convert_to_scalar(dynamic_to_time_internal, agt, "time"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_TIME;
    gtv.val.int_value = (int64)t;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}   

PG_FUNCTION_INFO_V1(totimetz);            
Datum totimetz(PG_FUNCTION_ARGS)
{   
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    TimeTzADT* t;

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    t = DatumGetInt64(convert_to_scalar(dynamic_to_timetz_internal, agt, "time"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_TIMETZ;
    gtv.val.timetz.time = t->time;
    gtv.val.timetz.zone = t->zone;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
} 

PG_FUNCTION_INFO_V1(tointerval);
Datum tointerval(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    Interval *i;

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    i = DatumGetIntervalP(convert_to_scalar(dynamic_to_interval_internal, agt, "interval"));
   
    dynamic_value gtv; 
    gtv.type = DYNAMIC_INTERVAL;
    gtv.val.interval.time = i->time;
    gtv.val.interval.day = i->day;
    gtv.val.interval.month = i->month;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(todate);
Datum todate(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    DateADT dte = DatumGetInt64(convert_to_scalar(dynamic_to_date_internal, agt, "date"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_DATE;
    gtv.val.date = dte;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}  

Datum _dynamic_toinet(Datum arg){
    dynamic *agt = DATUM_GET_DYNAMIC_P(arg);

    if (is_dynamic_null(agt))
        return NULL;

    inet *i = DatumGetInetPP(convert_to_scalar(dynamic_to_inet_internal, agt, "inet"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_INET;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);

    return DYNAMIC_P_GET_DATUM(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_toinet);             
Datum dynamic_toinet(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    inet *i = DatumGetInetPP(convert_to_scalar(dynamic_to_inet_internal, agt, "inet"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_INET;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tocidr);
Datum dynamic_tocidr(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    inet *i = DatumGetInetPP(convert_to_scalar(dynamic_to_cidr_internal, agt, "cidr"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_CIDR;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tomacaddr);
Datum dynamic_tomacaddr(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    macaddr *mac = DatumGetMacaddrP(convert_to_scalar(dynamic_to_macaddr_internal, agt, "cidr"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_MAC;
    memcpy(&gtv.val.mac, mac, sizeof(char) * 6);


    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tomacaddr8);
Datum dynamic_tomacaddr8(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    macaddr8 *mac = DatumGetMacaddrP(convert_to_scalar(dynamic_to_macaddr8_internal, agt, "macaddr8"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_MAC8;
    memcpy(&gtv.val.mac, mac, sizeof(char) * 8);

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_topoint);
Datum dynamic_topoint(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Point *point = DatumGetPointer(convert_to_scalar(dynamic_to_point_internal, agt, "point"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_POINT;
    gtv.val.point = point;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tolseg);
Datum dynamic_tolseg(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    LSEG *lseg = DatumGetPointer(convert_to_scalar(dynamic_to_lseg_internal, agt, "lseg"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_LSEG;
    gtv.val.lseg = lseg;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_topath);
Datum dynamic_topath(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    PATH *path = DatumGetPointer(convert_to_scalar(dynamic_to_path_internal, agt, "path"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_PATH;
    gtv.val.path = path;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}


PG_FUNCTION_INFO_V1(dynamic_toline);
Datum dynamic_toline(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    LINE *line =  DatumGetPointer(convert_to_scalar(dynamic_to_line_internal, agt, "line"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_LINE;
    gtv.val.path = line;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_topolygon);
Datum dynamic_topolygon(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    POLYGON *polygon = DatumGetPointer(convert_to_scalar(dynamic_to_polygon_internal, agt, "polygon"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_POLYGON;
    gtv.val.polygon = polygon;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tocircle);
Datum dynamic_tocircle(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    CIRCLE *circle = DatumGetPointer(convert_to_scalar(dynamic_to_circle_internal, agt, "circle"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_CIRCLE;
    gtv.val.circle = circle;
    
    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}



PG_FUNCTION_INFO_V1(dynamic_tobox);
Datum dynamic_tobox(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    BOX *box = DatumGetPointer(convert_to_scalar(dynamic_to_box_internal, agt, "box"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_BOX;
    gtv.val.box = box;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_totsvector);
Datum dynamic_totsvector(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    TSVector tsvector = DatumGetPointer(convert_to_scalar(dynamic_to_tsvector_internal, agt, "tsvector"));

    dynamic_value gtv;
    gtv.type = DYNAMIC_TSVECTOR;
    gtv.val.range = tsvector;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_totsquery);
Datum dynamic_totsquery(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    TSQuery tsquery = DatumGetPointer(DirectFunctionCall1(tsqueryin,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string")));

    dynamic_value gtv;
    gtv.type = DYNAMIC_TSQUERY;
    gtv.val.tsquery = tsquery;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}


Datum
PgDynamicDirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2,
                                                Datum arg3)
{
        LOCAL_FCINFO(fcinfo, 3);
	fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
	
        Datum           result;

        InitFunctionCallInfoData(*fcinfo, NULL, 3, collation, NULL, NULL);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
        fcinfo->flinfo->fn_addr = func;
        fcinfo->flinfo->fn_oid = InvalidOid;
        fcinfo->flinfo->fn_strict = false;
        fcinfo->flinfo->fn_retset = false;
        fcinfo->flinfo->fn_extra = NULL;
        fcinfo->flinfo->fn_mcxt = CurrentMemoryContext;
        fcinfo->flinfo->fn_expr = NULL;

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].value = arg2;
        fcinfo->args[1].isnull = false;
        fcinfo->args[2].value = arg3;
        fcinfo->args[2].isnull = false;

        result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull)
                elog(ERROR, "function %p returned NULL", (void *) func);

        return result;
}

PG_FUNCTION_INFO_V1(dynamic_tointrange);
Datum dynamic_tointrange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();
						  
    RangeType *range = DatumGetPointer(convert_to_scalar(dynamic_to_int_range_internal, agt, "int_range"));
    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_INT;
    gtv.val.range = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tointmultirange);
Datum dynamic_tointmultirange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(INT8MULTIRANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_INT_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}


PG_FUNCTION_INFO_V1(dynamic_tonumrange);
Datum dynamic_tonumrange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    RangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(NUMRANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_NUM;
    gtv.val.range = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_tonummultirange);
Datum dynamic_tonummultirange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(NUMMULTIRANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_NUM_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_totsrange);
Datum dynamic_totsrange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    RangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(TSRANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_TS;
    gtv.val.range = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_totsmultirange);
Datum dynamic_totsmultirange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(TSMULTIRANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_TS_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_totstzrange);
Datum dynamic_totstzrange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    RangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(TSTZRANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_TS;
    gtv.val.range = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_totstzmultirange);
Datum dynamic_totstzmultirange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(TSTZMULTIRANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_TSTZ_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_todaterange);
/*
 * Execute function to typecast an adynamic to an adynamic timestamp
 */
Datum dynamic_todaterange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    RangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(DATERANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_DATE;
    gtv.val.range = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(dynamic_todatemultirange);
/*
 * Execute function to typecast an adynamic to an adynamic timestamp
 */
Datum dynamic_todatemultirange(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PgDynamicDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(dynamic_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(DATEMULTIRANGEOID), Int32GetDatum(1)));

    dynamic_value gtv;
    gtv.type = DYNAMIC_RANGE_DATE_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(dynamic_value_to_dynamic(&gtv));
}

/*
 * dynamic to postgres functions
 */
PG_FUNCTION_INFO_V1(dynamic_to_int8);
// dynamic -> int8.
Datum
dynamic_to_int8(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_int8_internal, agt, "int8");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_int4);
// dynamic -> int4
Datum
dynamic_to_int4(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_int4_internal, agt, "int4");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_int2);
// dynamic -> int2
Datum
dynamic_to_int2(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_int2_internal, agt, "int2");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}


PG_FUNCTION_INFO_V1(dynamic_to_float8);
// dynamic -> float8.
Datum
dynamic_to_float8(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_float8_internal, agt, "float8");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_numeric);
// dynamic -> numeric.
Datum
dynamic_to_numeric(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_numeric_internal, agt, "numerifloat8c");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_float4);
// dynamic -> float4.
Datum
dynamic_to_float4(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_float4_internal, agt, "float4");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_text);
// dynamic -> text
Datum
dynamic_to_text(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_string_internal, agt, "text");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_bool);
// dynamic -> boolean
Datum dynamic_to_bool(PG_FUNCTION_ARGS)
{
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_boolean_internal, agt, "bool");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_timestamp);
// dynamic -> timestamp
Datum
dynamic_to_timestamp(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_timestamp_internal, agt, "interval");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_timestamptz);
// dynamic -> timestamptz
Datum
dynamic_to_timestamptz(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_timestamptz_internal, agt, "timestamptz");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_date);
// dynamic -> date
Datum
dynamic_to_date(PG_FUNCTION_ARGS) { 
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_date_internal, agt, "date");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_time);
// dynamic -> time
Datum
dynamic_to_time(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_time_internal, agt, "time");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_timetz);
// dynamic -> timetz
Datum
dynamic_to_timetz(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_timetz_internal, agt, "timetz");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_interval);
// dynamic -> interval
Datum
dynamic_to_interval(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_interval_internal, agt, "interval");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}


PG_FUNCTION_INFO_V1(dynamic_to_inet);
// dynamic -> inet
Datum
dynamic_to_inet(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_inet_internal, agt, "inet");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_cidr);
// dynamic -> cidr
Datum
dynamic_to_cidr(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_cidr_internal, agt, "cidr");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_point);
// dynamic -> point
Datum
dynamic_to_point(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();
    
    Datum d = convert_to_scalar(dynamic_to_point_internal, agt, "point");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_path);
// dynamic -> path
Datum
dynamic_to_path(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_path_internal, agt, "path");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_polygon);
// dynamic -> polygon
Datum
dynamic_to_polygon(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_polygon_internal, agt, "polygon");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_box);
// dynamic -> box
Datum
dynamic_to_box(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_box_internal, agt, "box");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}


PG_FUNCTION_INFO_V1(dynamic_to_tsvector);
// dynamic -> tsvector
Datum
dynamic_to_tsvector(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_tsvector_internal, agt, "tsvector");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(dynamic_to_tsquery);
// dynamic -> tsquery
Datum
dynamic_to_tsquery(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    if (is_dynamic_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(dynamic_to_tsquery_internal, agt, "tsquery");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

/*
 * Postgres types to dynamic
 */

PG_FUNCTION_INFO_V1(bool_to_dynamic);
// boolean -> dynamic
Datum
bool_to_dynamic(PG_FUNCTION_ARGS) {
    AG_RETURN_DYNAMIC_P(boolean_to_dynamic(PG_GETARG_BOOL(0)));
}

PG_FUNCTION_INFO_V1(float8_to_dynamic);
// float8 -> dynamic
Datum
float8_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    dynamic *agt;

    gtv.type = DYNAMIC_FLOAT;
    gtv.val.float_value = PG_GETARG_FLOAT8(0);
    agt = dynamic_value_to_dynamic(&gtv);

    AG_RETURN_DYNAMIC_P(agt);    
}

PG_FUNCTION_INFO_V1(int8_to_dynamic);
// int8 -> dynamic.
Datum
int8_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    dynamic *agt;

    gtv.type = DYNAMIC_INTEGER;
    gtv.val.int_value = PG_GETARG_INT64(0);
    agt = dynamic_value_to_dynamic(&gtv);

    AG_RETURN_DYNAMIC_P(agt);
}

PG_FUNCTION_INFO_V1(text_to_dynamic);
//text -> dynamic
Datum
text_to_dynamic(PG_FUNCTION_ARGS) {
    Datum txt = PG_GETARG_DATUM(0);
    dynamic_value gtv;
    dynamic *agt;

    gtv.type = DYNAMIC_STRING;
    gtv.val.string.len = check_string_length(strlen(TextDatumGetCString(txt)));
    gtv.val.string.val = TextDatumGetCString(txt);
    agt = dynamic_value_to_dynamic(&gtv);

    AG_RETURN_DYNAMIC_P(agt);
}

PG_FUNCTION_INFO_V1(timestamp_to_dynamic);
//timestamp -> dynamic
Datum
timestamp_to_dynamic(PG_FUNCTION_ARGS) {
    int64 ts = PG_GETARG_TIMESTAMP(0);

    dynamic_value gtv;
    gtv.type = DYNAMIC_TIMESTAMP;
    gtv.val.int_value = ts;

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(timestamptz_to_dynamic);
//timestamptz -> dynamic
Datum
timestamptz_to_dynamic(PG_FUNCTION_ARGS) {
    int64 ts = PG_GETARG_TIMESTAMPTZ(0);
    
    dynamic_value gtv;
    gtv.type = DYNAMIC_TIMESTAMPTZ;
    gtv.val.int_value = ts;

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(date_to_dynamic);
//date -> dynamic
Datum
date_to_dynamic(PG_FUNCTION_ARGS) {

    dynamic_value gtv;
    gtv.type = DYNAMIC_DATE;
    gtv.val.date = PG_GETARG_DATEADT(0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(time_to_dynamic);
//time -> dynamic
Datum
time_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    gtv.type = DYNAMIC_TIME;
    gtv.val.int_value = PG_GETARG_TIMEADT(0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(timetz_to_dynamic);
//timetz -> dynamic
Datum
timetz_to_dynamic(PG_FUNCTION_ARGS) {
    TimeTzADT *t = PG_GETARG_TIMETZADT_P(0);

    dynamic_value gtv;
    gtv.type = DYNAMIC_TIMETZ;
    gtv.val.timetz.time = t->time;
    gtv.val.timetz.zone = t->zone;

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(interval_to_dynamic);
//interval -> dynamic
Datum
interval_to_dynamic(PG_FUNCTION_ARGS) {
    Interval *i = PG_GETARG_INTERVAL_P(0);

    dynamic_value gtv;
    gtv.type = DYNAMIC_INTERVAL;
    gtv.val.interval.time = i->time;
    gtv.val.interval.day = i->day;
    gtv.val.interval.month = i->month;

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(inet_to_dynamic);
//inet -> dynamic
Datum
inet_to_dynamic(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);

    dynamic_value gtv;
    gtv.type = DYNAMIC_INET;
    memcpy(&gtv.val.inet, ip, sizeof(char) * 22);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(cidr_to_dynamic);
//cidr -> dynamic
Datum
cidr_to_dynamic(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);

    dynamic_value gtv;
    gtv.type = DYNAMIC_CIDR;
    memcpy(&gtv.val.inet, ip, sizeof(char) * 22);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(point_to_dynamic);
//point -> dynamic
Datum
point_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    gtv.type = DYNAMIC_POINT;
    gtv.val.range = PG_GETARG_POINTER(0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(path_to_dynamic);
//path -> dynamic
Datum
path_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    gtv.type = DYNAMIC_POINT;
    gtv.val.range = PG_GETARG_POINTER(0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}


PG_FUNCTION_INFO_V1(polygon_to_dynamic);
//polygon -> dynamic
Datum
polygon_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    gtv.type = DYNAMIC_POLYGON;
    gtv.val.range = PG_GETARG_POINTER(0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(box_to_dynamic);
//box -> dynamic
Datum
box_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    gtv.type = DYNAMIC_BOX;
    gtv.val.range = PG_GETARG_POINTER(0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}


PG_FUNCTION_INFO_V1(tsvector_to_dynamic);
//tsvector -> dynamic
Datum
tsvector_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    gtv.type = DYNAMIC_TSVECTOR;
    gtv.val.tsvector = PG_GETARG_POINTER(0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

PG_FUNCTION_INFO_V1(tsquery_to_dynamic);
//tsquery -> dynamic
Datum
tsquery_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_value gtv;
    gtv.type = DYNAMIC_TSQUERY;
    gtv.val.tsquery = PG_GETARG_POINTER(0);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(&gtv));
}

/*
 * postgres array to dynamic list
 */
PG_FUNCTION_INFO_V1(array_to_dynamic);
Datum
array_to_dynamic(PG_FUNCTION_ARGS) {
    dynamic_in_state in_state;
    memset(&in_state, 0, sizeof(dynamic_in_state));

    array_to_dynamic_internal(PG_GETARG_DATUM(0), &in_state);

    AG_RETURN_DYNAMIC_P(dynamic_value_to_dynamic(in_state.res));
}

/*
 * dynamic to postgres array functions
 */
PG_FUNCTION_INFO_V1(dynamic_to_text_array);
// dynamic -> text[]
Datum
dynamic_to_text_array(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    ArrayType *result = dynamic_to_array(dynamic_to_text_internal, agt, "text[]", TEXTOID, -1, false);

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(dynamic_to_float8_array);
// dynamic -> float8[]
Datum 
dynamic_to_float8_array(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    ArrayType *result = dynamic_to_array(dynamic_to_float8_internal, agt, "float8[]", FLOAT8OID, 8, true);

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(dynamic_to_float4_array);
// dynamic -> float4[]
Datum
dynamic_to_float4_array(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    ArrayType *result = dynamic_to_array(dynamic_to_float4_internal, agt, "float4[]", FLOAT4OID, 4, true);

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(dynamic_to_numeric_array);
// dynamic -> numeric[]
Datum
dynamic_to_numeric_array(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    ArrayType *result = dynamic_to_array(dynamic_to_numeric_internal, agt, "numeric[]", NUMERICOID, -1, false);

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(dynamic_to_int8_array);
// dynamic -> int8[]
Datum
dynamic_to_int8_array(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);

    ArrayType *result = dynamic_to_array(dynamic_to_int8_internal, agt, "int8[]", INT8OID, 8, true);

    PG_FREE_IF_COPY(agt, 0);
    
    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(dynamic_to_int4_array);
// dynamic -> int4[]
Datum dynamic_to_int4_array(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    
    ArrayType *result = dynamic_to_array(dynamic_to_int4_internal, agt, "int4[]", INT4OID, 4, true);

    PG_FREE_IF_COPY(agt, 0);
    
    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(dynamic_to_int2_array);
// dynamic -> int2[]
Datum dynamic_to_int2_array(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    
    ArrayType *result = dynamic_to_array(dynamic_to_int2_internal, agt, "int2[]", INT4OID, 4, true);

    PG_FREE_IF_COPY(agt, 0);
    
    PG_RETURN_ARRAYTYPE_P(result);
}

static ArrayType *
dynamic_to_array(coearce_function func, dynamic *agt, char *type, Oid type_oid, int type_len, bool elembyval) {
    dynamic_value gtv;
    Datum *array_value;

    dynamic_iterator *dynamic_iterator = dynamic_iterator_init(&agt->root);
    dynamic_iterator_token gtoken = dynamic_iterator_next(&dynamic_iterator, &gtv, false);

    if (gtv.type != DYNAMIC_ARRAY)
        cannot_cast_dynamic_value(gtv.type, type);

    array_value = (Datum *) palloc(sizeof(Datum) * DYNA_ROOT_COUNT(agt));

    int i = 0;
    while ((gtoken = dynamic_iterator_next(&dynamic_iterator, &gtv, true)) < WGT_END_ARRAY)
        array_value[i++] = func(&gtv);

    ArrayType *result = construct_array(array_value, DYNA_ROOT_COUNT(agt), type_oid, type_len, elembyval, 'i');

    return result;
}

/*
 * internal scalar conversion functions
 */
Datum
dynamic_to_int8_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTEGER)
        return Int64GetDatum(gtv->val.int_value);
    else if (gtv->type == DYNAMIC_FLOAT)
        return DirectFunctionCall1(float8_to_int8, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == DYNAMIC_NUMERIC)
        return DirectFunctionCall1(numeric_to_int8, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(string_to_int8, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "int8");

    // cannot reach
    return 0;
}

Datum
dynamic_to_int_range_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_RANGE_INT)
        return PointerGetDatum(gtv->val.range);
    else if (gtv->type == DYNAMIC_STRING)
        return DatumGetPointer(PgDynamicDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  CStringGetDatum(gtv->val.string.val),
                                                  ObjectIdGetDatum(INT8RANGEOID), Int32GetDatum(1)));
    else
        cannot_cast_dynamic_value(gtv->type, "int8");

    // cannot reach
    return 0;
}

Datum
dynamic_to_int4_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTEGER)
        return DirectFunctionCall1(int8_to_int4, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == DYNAMIC_FLOAT)
        return DirectFunctionCall1(float8_to_int4, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == DYNAMIC_NUMERIC)
        return DirectFunctionCall1(numeric_to_int4, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(string_to_int4, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "int4");

    // cannot reach
    return 0;
}

Datum
dynamic_to_int2_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTEGER)
        return DirectFunctionCall1(int8_to_int2, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == DYNAMIC_FLOAT)
        return DirectFunctionCall1(float8_to_int2, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == DYNAMIC_NUMERIC)
        return DirectFunctionCall1(numeric_to_int2, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(string_to_int2, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "int2");

    // cannot reach
    return 0;
}

Datum
dynamic_to_float8_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_FLOAT)
        return Float8GetDatum(gtv->val.float_value);
    else if (gtv->type == DYNAMIC_INTEGER)
        return DirectFunctionCall1(i8tod, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == DYNAMIC_NUMERIC)
        return DirectFunctionCall1(numeric_float8, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(float8in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "float8");

    // unreachable
    return 0;
}


Datum
dynamic_to_boolean_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_BOOL) {
        return BoolGetDatum(gtv->val.boolean);
    } else if (gtv->type == DYNAMIC_STRING) {
        char *string = gtv->val.string.val;
        int len = gtv->val.string.len;

        if (!pg_strncasecmp(string, "true", len))
            return BoolGetDatum(true);
        else if (!pg_strncasecmp(string, "false", len))
            return BoolGetDatum(false);
    }

    cannot_cast_dynamic_value(gtv->type, "boolean");

    // unreachable
    return 0;
}


Datum
dynamic_to_float4_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_FLOAT)
        return DirectFunctionCall1(dtof, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == DYNAMIC_INTEGER)
        return DirectFunctionCall1(i8tof, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == DYNAMIC_NUMERIC)
        return DirectFunctionCall1(numeric_float4, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(float4in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "float4");

    // unreachable
    return 0;
}

Datum
dynamic_to_numeric_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTEGER)
        return DirectFunctionCall1(int8_to_numeric, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == DYNAMIC_FLOAT)
        return DirectFunctionCall1(float8_to_numeric, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == DYNAMIC_NUMERIC)
        return NumericGetDatum(gtv->val.numeric);
    else if (gtv->type == DYNAMIC_STRING) {
        char *string = (char *) palloc(sizeof(char) * (gtv->val.string.len + 1));
        string = strncpy(string, gtv->val.string.val, gtv->val.string.len);
        string[gtv->val.string.len] = '\0';

        Datum numd = DirectFunctionCall3(string_to_numeric, CStringGetDatum(string), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

        pfree(string);

        return numd;
    } else
        cannot_cast_dynamic_value(gtv->type, "numeric");

    // unreachable
    return 0;
}


Datum
dynamic_to_text_internal(dynamic_value *gtv) {
    return CStringGetTextDatum(DatumGetCString(dynamic_to_string_internal(gtv)));
}

Datum
dynamic_to_string_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTEGER)
        return DirectFunctionCall1(int8_to_string, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == DYNAMIC_FLOAT)
        return DirectFunctionCall1(float8_to_string, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == DYNAMIC_STRING)
        return CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len));
    else if (gtv->type == DYNAMIC_NUMERIC)
        return DirectFunctionCall1(numeric_to_string, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == DYNAMIC_BOOL)
        return CStringGetDatum((gtv->val.boolean) ? "true" : "false");
    else if (gtv->type == DYNAMIC_TIMESTAMP)
        return DirectFunctionCall1(timestamp_out, TimestampGetDatum(gtv->val.int_value));
    else
        cannot_cast_dynamic_value(gtv->type, "string");

    // unreachable
    return CStringGetDatum("");
}

Datum
dynamic_to_timestamp_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTEGER || gtv->type == DYNAMIC_TIMESTAMP)
        return TimestampGetDatum(gtv->val.int_value);
    else if (gtv->type == DYNAMIC_STRING)
        return TimestampGetDatum(DirectFunctionCall3(timestamp_in,
				          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
					  ObjectIdGetDatum(InvalidOid),
					  Int32GetDatum(-1)));
    else if (gtv->type == DYNAMIC_TIMESTAMPTZ)
        return DirectFunctionCall1(timestamptz_timestamp, TimestampTzGetDatum(gtv->val.int_value));
    else if (gtv->type == DYNAMIC_DATE)
        return TimestampGetDatum(date2timestamp_opt_overflow(gtv->val.date, NULL));
    else
        cannot_cast_dynamic_value(gtv->type, "timestamp");

    // unreachable
    return CStringGetDatum("");
}

Datum
dynamic_to_timestamptz_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTEGER || gtv->type == DYNAMIC_TIMESTAMPTZ)
        return TimestampGetDatum(gtv->val.int_value);
    else if (gtv->type == DYNAMIC_STRING)
        return TimestampTzGetDatum(DirectFunctionCall3(timestamptz_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (gtv->type == DYNAMIC_TIMESTAMP)
        return DirectFunctionCall1(timestamp_timestamptz, TimestampGetDatum(gtv->val.int_value));
    else if (gtv->type == DYNAMIC_DATE)
        return TimestampGetDatum(date2timestamptz_opt_overflow(gtv->val.date, NULL));
    else
        cannot_cast_dynamic_value(gtv->type, "timestamptz");

    // unreachable
    return CStringGetDatum("");
}


Datum
dynamic_to_date_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_TIMESTAMPTZ)
        return DateADTGetDatum(DirectFunctionCall1(timestamptz_date, TimestampTzGetDatum(gtv->val.int_value)));
    else if (gtv->type == DYNAMIC_STRING)
        return DateADTGetDatum(DirectFunctionCall3(date_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (gtv->type == DYNAMIC_TIMESTAMP)
	return DateADTGetDatum(DirectFunctionCall1(timestamp_date, TimestampGetDatum(gtv->val.int_value)));
    else if (gtv->type == DYNAMIC_DATE)
        return DateADTGetDatum(gtv->val.date);
    else
        cannot_cast_dynamic_value(gtv->type, "timestamptz");

    // unreachable
    return CStringGetDatum("");
}

Datum
dynamic_to_time_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_TIMESTAMPTZ)
        return TimeADTGetDatum(DirectFunctionCall1(timestamptz_time, TimestampTzGetDatum(gtv->val.int_value)));
    else if (gtv->type == DYNAMIC_STRING)
        return TimeADTGetDatum(DirectFunctionCall3(time_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (gtv->type == DYNAMIC_TIMESTAMP)
        return TimeADTGetDatum(DirectFunctionCall1(timestamp_time, TimestampGetDatum(gtv->val.int_value)));
    else if (gtv->type == DYNAMIC_INTERVAL)
        return TimeADTGetDatum(DirectFunctionCall1(interval_time, IntervalPGetDatum(&gtv->val.interval)));
    else if (gtv->type == DYNAMIC_TIMETZ)
	return TimeADTGetDatum(DirectFunctionCall1(timetz_time, TimeTzADTPGetDatum(&gtv->val.timetz)));
    else if (gtv->type == DYNAMIC_TIME)
	    return TimeADTGetDatum(gtv->val.int_value);
    else
        cannot_cast_dynamic_value(gtv->type, "time");

    // unreachable
    return CStringGetDatum("");
}

Datum
dynamic_to_timetz_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_TIMESTAMPTZ)
        return TimeTzADTPGetDatum(DirectFunctionCall1(timestamptz_timetz, TimestampTzGetDatum(gtv->val.int_value)));
    else if (gtv->type == DYNAMIC_STRING)
        return TimeTzADTPGetDatum(DirectFunctionCall3(timetz_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (gtv->type == DYNAMIC_TIME)
        return TimeTzADTPGetDatum(DirectFunctionCall1(time_timetz, TimeADTGetDatum(gtv->val.int_value)));
    else if (gtv->type == DYNAMIC_TIMETZ)
	return TimeTzADTPGetDatum(&gtv->val.timetz);
    else                 
        cannot_cast_dynamic_value(gtv->type, "time");
    
    // unreachable
    return CStringGetDatum("");
}  

Datum
dynamic_to_interval_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_INTERVAL)
        return IntervalPGetDatum(&gtv->val.interval);
    else if (gtv->type == DYNAMIC_STRING)
        return IntervalPGetDatum(DirectFunctionCall3(interval_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else                 
        cannot_cast_dynamic_value(gtv->type, "interval");
    
    // unreachable
    return CStringGetDatum("");
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

Datum
dynamic_to_cidr_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_CIDR)
        return InetPGetDatum(&gtv->val.inet);
    else if (gtv->type == DYNAMIC_INET)
	return DirectFunctionCall1(inet_to_cidr, InetPGetDatum(&gtv->val.inet));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(cidr_in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "cidr");
    
    // unreachable
    return CStringGetDatum("");
}   

Datum
dynamic_to_macaddr_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_MAC)
	return MacaddrPGetDatum(&gtv->val.mac);
    else if (gtv->type == DYNAMIC_MAC8)
        return DirectFunctionCall1(macaddr8tomacaddr, Macaddr8PGetDatum(&gtv->val.mac8));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(macaddr_in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "macaddr");

    // unreachable
    return CStringGetDatum("");
}

Datum
dynamic_to_macaddr8_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_MAC8)
        return Macaddr8PGetDatum(&gtv->val.mac8);
    else if (gtv->type == DYNAMIC_MAC)
        return DirectFunctionCall1(macaddrtomacaddr8, MacaddrPGetDatum(&gtv->val.mac));
    else if (gtv->type == DYNAMIC_STRING)
        return DirectFunctionCall1(macaddr8_in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_dynamic_value(gtv->type, "macaddr");

    // unreachable
    return CStringGetDatum("");
}


Datum
dynamic_to_box_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_BOX) {
        return PointerGetDatum(gtv->val.box);
    } else if (gtv->type == DYNAMIC_STRING) {
        return DirectFunctionCall1(box_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "box");

    // unreachable
    return CStringGetDatum("");
}


Datum
dynamic_to_point_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_POINT) {
        return PointerGetDatum(gtv->val.point);
    } else if (gtv->type == DYNAMIC_STRING){
        return DirectFunctionCall1(point_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "point");

    // unreachable
    return CStringGetDatum("");
}


Datum
dynamic_to_lseg_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_LSEG) {
        return PointerGetDatum(gtv->val.lseg);
    } else if (gtv->type == DYNAMIC_STRING){
        return DirectFunctionCall1(lseg_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "LSeg");

    // unreachable
    return CStringGetDatum("");
}

Datum
dynamic_to_line_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_LINE) {
        return PointerGetDatum(gtv->val.line);
    } else if (gtv->type == DYNAMIC_STRING){
        return DirectFunctionCall1(line_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "Line");

    // unreachable
    return CStringGetDatum("");
}


PG_FUNCTION_INFO_V1(geometry_to_path);

Datum
dynamic_to_path_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_PATH) {
        return PointerGetDatum(gtv->val.path);
    } else if (gtv->type == DYNAMIC_STRING){
        return DirectFunctionCall1(path_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "path");

    // unreachable
    return CStringGetDatum("");
}

PG_FUNCTION_INFO_V1(geometry_to_polygon);

Datum
dynamic_to_polygon_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_POLYGON) {
        return PointerGetDatum(gtv->val.polygon);
    } else if (gtv->type == DYNAMIC_STRING){
        return DirectFunctionCall1(poly_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "polygon");

    // unreachable
    return CStringGetDatum("");
}

Datum
dynamic_to_circle_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_CIRCLE) {
        return PointerGetDatum(gtv->val.circle);
    } else if (gtv->type == DYNAMIC_STRING){
        return DirectFunctionCall1(circle_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "Circle");

    // unreachable
    return CStringGetDatum("");
}


Datum
dynamic_to_tsvector_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_TSVECTOR) {
        return PointerGetDatum(gtv->val.tsvector);
    } else if (gtv->type == DYNAMIC_STRING){
        return DirectFunctionCall1(tsvectorin, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "TSVector");

    // unreachable
    return CStringGetDatum("");
}

Datum
dynamic_to_tsquery_internal(dynamic_value *gtv) {
    if (gtv->type == DYNAMIC_TSQUERY) {
        return PointerGetDatum(gtv->val.tsquery);
    } else if (gtv->type == DYNAMIC_STRING){
        return DirectFunctionCall1(tsqueryin, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_dynamic_value(gtv->type, "TSQuery");

    // unreachable
    return CStringGetDatum(""); 
}


/*
 * Emit correct, translatable cast error message
 */
static void
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
