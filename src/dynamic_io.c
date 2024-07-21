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
 * I/O routines for dynamic type
 */

#include "postgres.h"

#include <math.h>


#include "varatt.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_aggregate_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_operator_d.h"
#include "executor/nodeAgg.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "portability/instr_time.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
//#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#include "utils/dynamic.h"
#include "utils/dynamic_parser.h"
#include "utils/dynamic_typecasting.h"

// State structure for Percentile aggregate functions 
typedef struct PercentileGroupAggState
{
    // percentile value 
    float8 percentile;
    // Sort object we're accumulating data in: 
    Tuplesortstate *sortstate;
    // Number of normal rows inserted into sortstate: 
    int64 number_of_rows;
    // Have we already done tuplesort_performsort? 
    bool sort_done;
} PercentileGroupAggState;

typedef enum // type categories for   
{
    AGT_TYPE_NULL, // null, so we didn't bother to identify 
    AGT_TYPE_BOOL, // boolean (built-in types only) 
    AGT_TYPE_INTEGER, // Cypher Integer type 
    AGT_TYPE_FLOAT, // Cypher Float type 
    AGT_TYPE_NUMERIC, // numeric (ditto) 
    AGT_TYPE_DATE, // we use special formatting for datetimes 
    AGT_TYPE_TIMESTAMP, // we use special formatting for timestamp 
    AGT_TYPE_TIMESTAMPTZ, // ... and timestamptz 
    AGT_TYPE_TIME,
    AGT_TYPE_TIMETZ,
    AGT_TYPE_INTERVAL,
    AGT_TYPE_VECTOR,
    AGT_TYPE_INET,
    AGT_TYPE_CIDR,
    AGT_TYPE_MAC,
    AGT_TYPE_MAC8,
    AGT_TYPE_DYNAMIC, // DYNAMIC 
    AGT_TYPE_JSON, // JSON 
    AGT_TYPE_JSONB, // JSONB 
    AGT_TYPE_ARRAY, // array 
    AGT_TYPE_COMPOSITE, // composite 
    AGT_TYPE_JSONCAST, // something with an explicit cast to JSON 
    AGT_TYPE_VERTEX,
    AGT_TYPE_OTHER // all else 
} agt_type_category;

size_t check_string_length(size_t len);
static void dynamic_in_object_start(void *pstate);
static void dynamic_in_object_end(void *pstate);
static void dynamic_in_array_start(void *pstate);
static void dynamic_in_array_end(void *pstate);
static void dynamic_in_object_field_start(void *pstate, char *fname, bool isnull);
static void dynamic_put_array(StringInfo out, dynamic_value *scalar_val);
static void dynamic_put_object(StringInfo out, dynamic_value *scalar_val);
static void escape_dynamic(StringInfo buf, const char *str);
bool is_decimal_needed(char *numstr);
static void dynamic_in_scalar(void *pstate, char *token, dynamic_token_type tokentype, char *annotation);
static char *dynamic_to_cstring_worker(StringInfo out, dynamic_container *in, int estimated_len, bool indent);
static void add_indent(StringInfo out, bool indent, int level);
static dynamic_value *execute_array_access_operator_internal(dynamic *array, int64 array_index);
static dynamic_iterator *get_next_object_key(dynamic_iterator *it, dynamic_container *agtc, dynamic_value *key);
static Datum process_access_operator_result(FunctionCallInfo fcinfo, dynamic_value *agtv, bool as_text);
Datum dynamic_array_element_impl(FunctionCallInfo fcinfo, dynamic *dynamic_in, int element, bool as_text);

PG_FUNCTION_INFO_V1(dynamic_build_map_noargs);
/*              
 * degenerate case of dynamic_build_map where it gets 0 arguments.
 */                 
Datum dynamic_build_map_noargs(PG_FUNCTION_ARGS)
{   
    dynamic_in_state result;
    
    memset(&result, 0, sizeof(dynamic_in_state));
                                          
    push_dynamic_value(&result.parse_state, WGT_BEGIN_OBJECT, NULL);
    result.res = push_dynamic_value(&result.parse_state, WGT_END_OBJECT, NULL); 
                
    PG_RETURN_POINTER(dynamic_value_to_dynamic(result.res));
}    

// fast helper function to test for DYNAMIC_NULL in an dynamic 
bool is_dynamic_null(dynamic *agt_arg)
{
    dynamic_container *agtc = &agt_arg->root;

    if (DYNAMIC_CONTAINER_IS_SCALAR(agtc) && GTE_IS_NULL(agtc->children[0]))
        return true;

    return false;
}

bool is_dynamic_integer(dynamic *agt) {
    return DYNA_ROOT_IS_SCALAR(agt) && GTE_IS_DYNAMIC(agt->root.children[0]) && DYNA_IS_INTEGER(agt->root.children[1]);
}

bool is_dynamic_float(dynamic *agt) {
    return DYNA_ROOT_IS_SCALAR(agt) && GTE_IS_DYNAMIC(agt->root.children[0]) && DYNA_IS_FLOAT(agt->root.children[1]);
}

bool is_dynamic_numeric(dynamic *agt) {
    /*if (!DYNA_ROOT_IS_SCALAR(agt))
        return false;

    dynamic_value *gtv = get_ith_dynamic_value_from_container(&agt->root, 0);

    if (gtv->type == DYNAMIC_NUMERIC)
        return true;

    return false;*/
    return DYNA_ROOT_IS_SCALAR(agt) && GTE_IS_NUMERIC(agt->root.children[0]);
}

bool is_dynamic_string(dynamic *agt) {
    return DYNA_ROOT_IS_SCALAR(agt) && GTE_IS_STRING(agt->root.children[0]);
}

/*
 * dynamic recv function copied from PGs jsonb_recv as dynamic is based
 * off of jsonb
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
PG_FUNCTION_INFO_V1(dynamic_recv);

Datum dynamic_recv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    int version = pq_getmsgint(buf, 1);
    char *str = NULL;
    int nbytes = 0;

    if (version == 1)
        str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    else
        elog(ERROR, "unsupported dynamic version number %d", version);

    return dynamic_from_cstring(str, nbytes);
}

/*
 * dynamic send function copied from PGs jsonb_send as dynamic is based
 * off of jsonb
 *
 * Just send dynamic as a version number, then a string of text
 */
PG_FUNCTION_INFO_V1(dynamic_send);
Datum dynamic_send(PG_FUNCTION_ARGS) {
    dynamic *agt = AG_GET_ARG_DYNAMIC_P(0);
    StringInfoData buf;
    StringInfo dynamic_text = makeStringInfo();
    int version = 1;

    (void) dynamic_to_cstring(dynamic_text, &agt->root, VARSIZE(agt));

    pq_begintypsend(&buf);
    pq_sendint8(&buf, version);
    pq_sendtext(&buf, dynamic_text->data, dynamic_text->len);
    pfree(dynamic_text->data);
    pfree(dynamic_text);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(dynamic_in);
/*
 * dynamic type input function
 */
Datum dynamic_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);

    return dynamic_from_cstring(str, strlen(str));
}

PG_FUNCTION_INFO_V1(dynamic_out);
/*
 * dynamic type output function
 */
Datum dynamic_out(PG_FUNCTION_ARGS) {
    dynamic *agt = NULL;
    char *out = NULL;

    agt = AG_GET_ARG_DYNAMIC_P(0);

    out = dynamic_to_cstring(NULL, &agt->root, VARSIZE(agt));

    PG_RETURN_CSTRING(out);
}

/*
 * dynamic_from_cstring
 *
 * Turns dynamic string into an dynamic Datum.
 *
 * Uses the dynamic parser (with hooks) to construct an dynamic.
 */
Datum dynamic_from_cstring(char *str, int len)
{
    dynamic_lex_context *lex;
    dynamic_in_state state;
    dynamic_sem_action sem;

    memset(&state, 0, sizeof(state));
    memset(&sem, 0, sizeof(sem));
    lex = make_dynamic_lex_context_cstring_len(str, len, true);

    sem.semstate = (void *)&state;

    sem.object_start = dynamic_in_object_start;
    sem.array_start = dynamic_in_array_start;
    sem.object_end = dynamic_in_object_end;
    sem.array_end = dynamic_in_array_end;
    sem.scalar = dynamic_in_scalar;
    sem.object_field_start = dynamic_in_object_field_start;

    parse_dynamic(lex, &sem);

    // after parsing, the item member has the composed dynamic structure 
    PG_RETURN_POINTER(dynamic_value_to_dynamic(state.res));
}

size_t check_string_length(size_t len) {
    if (len > GTENTRY_OFFLENMASK)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("string too long to represent as dynamic string"),
                 errdetail("Due to an implementation restriction, dynamic strings cannot exceed %d bytes.", GTENTRY_OFFLENMASK)));

    return len;
}

static void dynamic_in_object_start(void *pstate) {
    dynamic_in_state *_state = (dynamic_in_state *)pstate;

    _state->res = push_dynamic_value(&_state->parse_state, WGT_BEGIN_OBJECT, NULL);
}

static void dynamic_in_object_end(void *pstate) {
    dynamic_in_state *_state = (dynamic_in_state *)pstate;

    _state->res = push_dynamic_value(&_state->parse_state, WGT_END_OBJECT, NULL);
}

static void dynamic_in_array_start(void *pstate) {
    dynamic_in_state *_state = (dynamic_in_state *)pstate;

    _state->res = push_dynamic_value(&_state->parse_state, WGT_BEGIN_ARRAY, NULL);
}

static void dynamic_in_array_end(void *pstate) {
    dynamic_in_state *_state = (dynamic_in_state *)pstate;

    _state->res = push_dynamic_value(&_state->parse_state, WGT_END_ARRAY, NULL);
}

static void dynamic_in_object_field_start(void *pstate, char *fname, bool isnull) {
    dynamic_in_state *_state = (dynamic_in_state *)pstate;
    dynamic_value v;

    Assert(fname != NULL);
    v.type = DYNAMIC_STRING;
    v.val.string.len = check_string_length(strlen(fname));
    v.val.string.val = fname;

    _state->res = push_dynamic_value(&_state->parse_state, WGT_KEY, &v);
}

Datum
PostGraphDirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1)
{
        LOCAL_FCINFO(fcinfo, 3);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));

        Datum           result;

        InitFunctionCallInfoData(*fcinfo, NULL, 1, collation, NULL, NULL);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
        fcinfo->flinfo->fn_addr = func;
        fcinfo->flinfo->fn_strict = false;
        fcinfo->flinfo->fn_retset = false;
        fcinfo->flinfo->fn_extra = NULL;
        fcinfo->flinfo->fn_mcxt = CurrentMemoryContext;
        fcinfo->flinfo->fn_expr = NULL;

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;

        result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull)
                elog(ERROR, "function %p returned NULL", (void *) func);

        return result;
}



void dynamic_put_escaped_value(StringInfo out, dynamic_value *scalar_val)
{
    char *numstr;

    switch (scalar_val->type)
    {
    case DYNAMIC_NULL:
        appendBinaryStringInfo(out, "null", 4);
        break;
    case DYNAMIC_STRING:
        escape_dynamic(out, pnstrdup(scalar_val->val.string.val, scalar_val->val.string.len));
        break;
    case DYNAMIC_NUMERIC:
        appendStringInfoString(
            out, DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(scalar_val->val.numeric))));
        appendBinaryStringInfo(out, "::numeric", 9);
        break;
    case DYNAMIC_INTEGER:
        appendStringInfoString(
            out, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(scalar_val->val.int_value))));
        break;
    case DYNAMIC_FLOAT:
        numstr = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(scalar_val->val.float_value)));
        appendStringInfoString(out, numstr);

        if (is_decimal_needed(numstr))
            appendBinaryStringInfo(out, ".0", 2);
        break;
    case DYNAMIC_TIMESTAMP:
	numstr = DatumGetCString(DirectFunctionCall1(timestamp_out, TimestampGetDatum(scalar_val->val.int_value)));
	appendStringInfoString(out, numstr);
	break;
    case DYNAMIC_TIMESTAMPTZ:
        numstr = DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampGetDatum(scalar_val->val.int_value)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_DATE:
        numstr = DatumGetCString(DirectFunctionCall1(date_out, DateADTGetDatum(scalar_val->val.date)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_TIME:
        numstr = DatumGetCString(DirectFunctionCall1(time_out, TimeADTGetDatum(scalar_val->val.int_value)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_TIMETZ:
        numstr = DatumGetCString(DirectFunctionCall1(timetz_out, DatumGetTimeTzADTP(&scalar_val->val.timetz)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_INTERVAL:
        numstr = DatumGetCString(DirectFunctionCall1(interval_out, IntervalPGetDatum(&scalar_val->val.interval)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_INET:
        numstr = DatumGetCString(DirectFunctionCall1(inet_out, InetPGetDatum(&scalar_val->val.inet)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_CIDR:
        numstr = DatumGetCString(DirectFunctionCall1(cidr_out, InetPGetDatum(&scalar_val->val.inet)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_MAC:
        numstr = DatumGetCString(DirectFunctionCall1(macaddr_out, MacaddrPGetDatum(&scalar_val->val.mac)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_MAC8:
        numstr = DatumGetCString(DirectFunctionCall1(macaddr8_out, MacaddrPGetDatum(&scalar_val->val.mac)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_POINT:
        numstr = DatumGetCString(DirectFunctionCall1(point_out, PointerGetDatum(scalar_val->val.point)));
        appendStringInfoString(out, numstr);
        break;	   
    case DYNAMIC_LSEG:
        numstr = DatumGetCString(DirectFunctionCall1(lseg_out, PointerGetDatum(scalar_val->val.lseg)));
        appendStringInfoString(out, numstr);
        break;	   
    case DYNAMIC_LINE:
        numstr = DatumGetCString(DirectFunctionCall1(line_out, PointerGetDatum(scalar_val->val.line)));
        appendStringInfoString(out, numstr);
        break;	   
    case DYNAMIC_PATH:
        numstr = DatumGetCString(DirectFunctionCall1(path_out, PointerGetDatum(scalar_val->val.path)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_POLYGON:
        numstr = DatumGetCString(DirectFunctionCall1(poly_out, PointerGetDatum(scalar_val->val.polygon)));
        appendStringInfoString(out, numstr);
        break;	 
    case DYNAMIC_CIRCLE:
        numstr = DatumGetCString(DirectFunctionCall1(circle_out, PointerGetDatum(scalar_val->val.circle)));
        appendStringInfoString(out, numstr);
        break;	    
    case DYNAMIC_BOX:
        numstr = DatumGetCString(DirectFunctionCall1(box_out, PointerGetDatum(scalar_val->val.box)));
        appendStringInfoString(out, numstr);
        break;	     
    case DYNAMIC_TSVECTOR:
        numstr = DatumGetCString(DirectFunctionCall1(tsvectorout, PointerGetDatum(scalar_val->val.tsvector)));
        appendStringInfoString(out, numstr);
        break;  
    case DYNAMIC_TSQUERY:
        numstr = DatumGetCString(DirectFunctionCall1(tsqueryout, PointerGetDatum(scalar_val->val.tsquery)));
        appendStringInfoString(out, numstr);
        break;	
    case DYNAMIC_BYTEA:
        numstr = DatumGetCString(DirectFunctionCall1(byteaout, PointerGetDatum(scalar_val->val.bytea)));
        appendStringInfoString(out, numstr);
        break;	      
    case DYNAMIC_RANGE_INT:
    case DYNAMIC_RANGE_NUM:
    case DYNAMIC_RANGE_TS:
    case DYNAMIC_RANGE_TSTZ:
    case DYNAMIC_RANGE_DATE:
	numstr = DatumGetCString(PostGraphDirectFunctionCall1Coll(range_out, DEFAULT_COLLATION_OID, PointerGetDatum(scalar_val->val.range)));
        appendStringInfoString(out, numstr);
        break;
    case DYNAMIC_RANGE_INT_MULTI:
    case DYNAMIC_RANGE_NUM_MULTI:
    case DYNAMIC_RANGE_TS_MULTI:
    case DYNAMIC_RANGE_TSTZ_MULTI:
    case DYNAMIC_RANGE_DATE_MULTI:
        numstr = DatumGetCString(PostGraphDirectFunctionCall1Coll(multirange_out, DEFAULT_COLLATION_OID, PointerGetDatum(scalar_val->val.multirange)));
        appendStringInfoString(out, numstr);
	break; 
    case DYNAMIC_BOOL:
        if (scalar_val->val.boolean)
            appendBinaryStringInfo(out, "true", 4);
        else
            appendBinaryStringInfo(out, "false", 5);
        break;
    default:
        elog(ERROR, "unknown dynamic scalar type");
    }
}

/*
 * Produce an dynamic string literal, properly escaping characters in the text.
 */
static void escape_dynamic(StringInfo buf, const char *str) {
    const char *p;

    appendStringInfoCharMacro(buf, '"');
    for (p = str; *p; p++)
    {
        switch (*p)
        {
        case '\b':
            appendStringInfoString(buf, "\\b");
            break;
        case '\f':
            appendStringInfoString(buf, "\\f");
            break;
        case '\n':
            appendStringInfoString(buf, "\\n");
            break;
        case '\r':
            appendStringInfoString(buf, "\\r");
            break;
        case '\t':
            appendStringInfoString(buf, "\\t");
            break;
        case '"':
            appendStringInfoString(buf, "\\\"");
            break;
        case '\\':
            appendStringInfoString(buf, "\\\\");
            break;
        default:
            if ((unsigned char)*p < ' ')
                appendStringInfo(buf, "\\u%04x", (int)*p);
            else
                appendStringInfoCharMacro(buf, *p);
            break;
        }
    }
    appendStringInfoCharMacro(buf, '"');
}

bool is_decimal_needed(char *numstr) {
    int i;

    Assert(numstr);

    i = (numstr[0] == '-') ? 1 : 0;

    while (numstr[i] != '\0') {
        if (numstr[i] < '0' || numstr[i] > '9')
            return false;

        i++;
    }

    return true;
}

/*
 * For dynamic we always want the de-escaped value - that's what's in token
 */
static void dynamic_in_scalar(void *pstate, char *token, dynamic_token_type tokentype, char *annotation) {
    dynamic_in_state *_state = (dynamic_in_state *)pstate;
    dynamic_value v;
    Datum numd;

    /*
     * Process the scalar typecast annotations, if present, but not if the
     * argument is a null. Typecasting a null is a null.
     */
    if (annotation != NULL && tokentype != DYNAMIC_TOKEN_NULL) {
        int len = strlen(annotation);

        if (pg_strcasecmp(annotation, "numeric") == 0)
            tokentype = DYNAMIC_TOKEN_NUMERIC;
        else if (pg_strcasecmp(annotation, "integer") == 0)
            tokentype = DYNAMIC_TOKEN_INTEGER;
        else if (pg_strcasecmp(annotation, "float") == 0)
            tokentype = DYNAMIC_TOKEN_FLOAT;
        else if (pg_strcasecmp(annotation, "timestamp") == 0)
            tokentype = DYNAMIC_TOKEN_TIMESTAMP;
        else if (len == 11 && pg_strcasecmp(annotation, "timestamptz") == 0)
            tokentype = DYNAMIC_TOKEN_TIMESTAMPTZ;
	else if (len == 4 && pg_strcasecmp(annotation, "date") == 0)
            tokentype = DYNAMIC_TOKEN_DATE;
        else if (len == 4 && pg_strcasecmp(annotation, "time") == 0)
            tokentype = DYNAMIC_TOKEN_TIME;
        else if (len == 6 && pg_strcasecmp(annotation, "timetz") == 0)
            tokentype = DYNAMIC_TOKEN_TIMETZ;
	else if (len == 8 && pg_strcasecmp(annotation, "interval") == 0)
            tokentype = DYNAMIC_TOKEN_INTERVAL;
        else if (len == 4 && pg_strcasecmp(annotation, "inet") == 0)
            tokentype = DYNAMIC_TOKEN_INET;
        else if (len == 4 && pg_strcasecmp(annotation, "cidr") == 0)
            tokentype = DYNAMIC_TOKEN_CIDR;
        else if (len == 7 && pg_strcasecmp(annotation, "macaddr") == 0)
            tokentype = DYNAMIC_TOKEN_MACADDR;
        else if (len == 8 && pg_strcasecmp(annotation, "macaddr8") == 0)
            tokentype = DYNAMIC_TOKEN_MACADDR;
	else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid annotation value for scalar")));
    }

    switch (tokentype)
    {
    case DYNAMIC_TOKEN_STRING:
        Assert(token != NULL);
        v.type = DYNAMIC_STRING;
        v.val.string.len = check_string_length(strlen(token));
        v.val.string.val = token;
        break;
    case DYNAMIC_TOKEN_INTEGER:
        Assert(token != NULL);
        v.type = DYNAMIC_INTEGER;
	    v.val.int_value = pg_strtoint64_safe(token, NULL);
        //scanint8(token, false, &v.val.int_value);
        break;
    case DYNAMIC_TOKEN_FLOAT:
        Assert(token != NULL);
        v.type = DYNAMIC_FLOAT;
	    v.val.float_value = float8in_internal(token, NULL, "double precision", token, NULL);
        break;
    case DYNAMIC_TOKEN_NUMERIC:
        Assert(token != NULL);
        v.type = DYNAMIC_NUMERIC;
        numd = DirectFunctionCall3(numeric_in,
			           CStringGetDatum(token),
				   ObjectIdGetDatum(InvalidOid),
				   Int32GetDatum(-1));
        v.val.numeric = DatumGetNumeric(numd);
        break;
    case DYNAMIC_TOKEN_TIMESTAMP:
        Assert(token != NULL);
        v.type = DYNAMIC_TIMESTAMP;
        v.val.int_value = DatumGetInt64(DirectFunctionCall3(timestamp_in,
				                            CStringGetDatum(token),
							    ObjectIdGetDatum(InvalidOid),
							    Int32GetDatum(-1)));
        break;
    case DYNAMIC_TOKEN_TIMESTAMPTZ:
        v.type = DYNAMIC_TIMESTAMPTZ;
        v.val.int_value = DatumGetInt64(DirectFunctionCall3(timestamptz_in,
                                                            CStringGetDatum(token),
                                                            ObjectIdGetDatum(InvalidOid),
                                                            Int32GetDatum(-1)));
        break;
    case DYNAMIC_TOKEN_DATE:
        v.type = DYNAMIC_DATE;
        v.val.date = DatumGetInt32(DirectFunctionCall1(date_in, CStringGetDatum(token)));
        break;
    case DYNAMIC_TOKEN_TIME:
        v.type = DYNAMIC_TIME;
        v.val.int_value = DatumGetInt64(DirectFunctionCall3(time_in,
                                                            CStringGetDatum(token),
                                                            ObjectIdGetDatum(InvalidOid),
                                                            Int32GetDatum(-1)));
	break;
    case DYNAMIC_TOKEN_TIMETZ:
        v.type = DYNAMIC_TIMETZ;
        TimeTzADT * timetz= DatumGetTimeTzADTP(DirectFunctionCall3(timetz_in,
                                                                   CStringGetDatum(token),
                                                                   ObjectIdGetDatum(InvalidOid),
                                                                   Int32GetDatum(-1)));
        v.val.timetz.time = timetz->time;
        v.val.timetz.zone = timetz->zone;
        break;
    case DYNAMIC_TOKEN_INTERVAL:
        {
        Interval *interval;

        Assert(token != NULL);

        v.type = DYNAMIC_INTERVAL;
        interval = DatumGetIntervalP(DirectFunctionCall3(interval_in,
				             CStringGetDatum(token),
                                             ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(-1)));

        v.val.interval.time = interval->time;
        v.val.interval.day = interval->day;
        v.val.interval.month = interval->month;
        break;
        }
    case DYNAMIC_TOKEN_INET:
        {
        inet *i;

        Assert(token != NULL);

        v.type = DYNAMIC_INET;
        i = DatumGetInetPP(DirectFunctionCall1(inet_in, CStringGetDatum(token)));

	memcpy(&v.val.inet, i, sizeof(char) * 22);
        break;
        }
    case DYNAMIC_TOKEN_CIDR:
        {
        inet *i;

        Assert(token != NULL);

        v.type = DYNAMIC_CIDR;
        i = DatumGetInetPP(DirectFunctionCall1(cidr_in, CStringGetDatum(token)));

        memcpy(&v.val.inet, i, sizeof(char) * 22);
        break;
        }
    case DYNAMIC_TOKEN_MACADDR:
        {
        macaddr *mac;

        Assert(token != NULL);

        v.type = DYNAMIC_MAC;
        mac = DatumGetMacaddrP(DirectFunctionCall1(macaddr_in, CStringGetDatum(token)));

        memcpy(&v.val.mac, mac, sizeof(char) * 6);
        break;
        }
    case DYNAMIC_TOKEN_MACADDR8:
        {
        macaddr8 *mac;

        Assert(token != NULL);

        v.type = DYNAMIC_MAC8;
        mac = DatumGetMacaddrP(DirectFunctionCall1(macaddr8_in, CStringGetDatum(token)));

        memcpy(&v.val.mac, mac, sizeof(char) * 8);
        break;
        }
    case DYNAMIC_TOKEN_TRUE:
        v.type = DYNAMIC_BOOL;
        v.val.boolean = true;
        break;
    case DYNAMIC_TOKEN_FALSE:
        v.type = DYNAMIC_BOOL;
        v.val.boolean = false;
        break;
    case DYNAMIC_TOKEN_NULL:
        v.type = DYNAMIC_NULL;
        break;
    default:
        // should not be possible 
        elog(ERROR, "invalid dynamic token type");
        break;
    }

    if (_state->parse_state == NULL) {
        // single scalar 
        dynamic_value va;

        va.type = DYNAMIC_ARRAY;
        va.val.array.raw_scalar = true;
        va.val.array.num_elems = 1;

        _state->res = push_dynamic_value(&_state->parse_state, WGT_BEGIN_ARRAY, &va);
        _state->res = push_dynamic_value(&_state->parse_state, WGT_ELEM, &v);
        _state->res = push_dynamic_value(&_state->parse_state, WGT_END_ARRAY, NULL);
    } else {
        dynamic_value *o = &_state->parse_state->cont_val;

        switch (o->type)
        {
        case DYNAMIC_ARRAY:
            _state->res = push_dynamic_value(&_state->parse_state, WGT_ELEM, &v);
            break;
        case DYNAMIC_OBJECT:
            _state->res = push_dynamic_value(&_state->parse_state, WGT_VALUE, &v);
            break;
        default:
            elog(ERROR, "unexpected parent of nested structure");
        }
    }
}

/*
 * dynamic_to_cstring
 *     Converts dynamic value to a C-string.
 *
 * If 'out' argument is non-null, the resulting C-string is stored inside the
 * StringBuffer.  The resulting string is always returned.
 *
 * A typical case for passing the StringInfo in rather than NULL is where the
 * caller wants access to the len attribute without having to call strlen, e.g.
 * if they are converting it to a text* object.
 */
char *dynamic_to_cstring(StringInfo out, dynamic_container *in, int estimated_len) {
    return dynamic_to_cstring_worker(out, in, estimated_len, false);
}

/*
 * same thing but with indentation turned on
 */
char *dynamic_to_cstring_indent(StringInfo out, dynamic_container *in, int estimated_len) {
    return dynamic_to_cstring_worker(out, in, estimated_len, true);
}

/*
 * common worker for above two functions
 */
static char *dynamic_to_cstring_worker(StringInfo out, dynamic_container *in, int estimated_len, bool indent)
{
    bool first = true;
    dynamic_iterator *it;
    dynamic_value v;
    dynamic_iterator_token type = WGT_DONE;
    int level = 0;
    bool redo_switch = false;

    // If we are indenting, don't add a space after a comma 
    int ispaces = indent ? 1 : 2;

    /*
     * Don't indent the very first item. This gets set to the indent flag at
     * the bottom of the loop.
     */
    bool use_indent = false;
    bool raw_scalar = false;
    bool last_was_key = false;

    if (out == NULL)
        out = makeStringInfo();

    enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64);

    it = dynamic_iterator_init(in);

    while (redo_switch ||
           ((type = dynamic_iterator_next(&it, &v, false)) != WGT_DONE))
    {
        redo_switch = false;
        switch (type)
        {
        case WGT_BEGIN_ARRAY:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);

            if (!v.val.array.raw_scalar) {
                add_indent(out, use_indent && !last_was_key, level);
                appendStringInfoCharMacro(out, '[');
            } else {
                raw_scalar = true;
            }

            first = true;
            level++;
            break;
        case WGT_BEGIN_OBJECT:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);

            add_indent(out, use_indent && !last_was_key, level);
            appendStringInfoCharMacro(out, '{');

            first = true;
            level++;
            break;
        case WGT_KEY:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);
            first = true;

            add_indent(out, use_indent, level);

            // dynamic rules guarantee this is a string 
            dynamic_put_escaped_value(out, &v);
            appendBinaryStringInfo(out, ": ", 2);

            type = dynamic_iterator_next(&it, &v, false);
            if (type == WGT_VALUE) {
                first = false;
                dynamic_put_escaped_value(out, &v);
            } else {
                Assert(type == WGT_BEGIN_OBJECT || type == WGT_BEGIN_ARRAY || type == WGT_BEGIN_VECTOR);

                /*
                 * We need to rerun the current switch() since we need to
                 * output the object which we just got from the iterator
                 * before calling the iterator again.
                 */
                redo_switch = true;
            }
            break;
        case WGT_ELEM:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);
            first = false;

            if (!raw_scalar)
                add_indent(out, use_indent, level);
            dynamic_put_escaped_value(out, &v);
            break;
        case WGT_END_ARRAY:
            level--;
            if (!raw_scalar) {
                add_indent(out, use_indent, level);
                appendStringInfoCharMacro(out, ']');
            }
            first = false;
            break;
        case WGT_END_OBJECT:
            level--;
            add_indent(out, use_indent, level);
            appendStringInfoCharMacro(out, '}');
            first = false;
            break;
        default:
            elog(ERROR, "unknown dynamic iterator token type");
        }
        use_indent = indent;
        last_was_key = redo_switch;
    }

    Assert(level == 0);

    return out->data;
}

static void add_indent(StringInfo out, bool indent, int level) {
    if (indent) {
        int i;

        appendStringInfoCharMacro(out, '\n');
        for (i = 0; i < level; i++)
            appendBinaryStringInfo(out, "    ", 4);
    }
}

