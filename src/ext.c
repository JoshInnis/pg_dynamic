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

#include "utils/dynamic_ext.h"
#include "utils/dynamic.h"

/* define the type and size of the agt_header */
#define DYNA_HEADER_TYPE uint32
#define DYNA_HEADER_SIZE sizeof(DYNA_HEADER_TYPE)

static short ag_serialize_header(StringInfo buffer, uint32 type)
{
    short padlen;
    int offset;

    padlen = pad_buffer_to_int(buffer);
    offset = reserve_from_buffer(buffer, DYNA_HEADER_SIZE);
    *((DYNA_HEADER_TYPE *)(buffer->data + offset)) = type;

    return padlen;
}

/*
 * Function serializes the data into the buffer provided.
 * Returns false if the type is not defined. Otherwise, true.
 */
bool ag_serialize_extended_type(StringInfo buffer, gtentry *gtentry,
                                dynamic_value *scalar_val)
{
    short padlen;
    int numlen;
    int offset;

    switch (scalar_val->type)
    {
    case DYNAMIC_INTEGER:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_INTEGER);

        /* copy in the int_value data */
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;

    case DYNAMIC_FLOAT:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_FLOAT);

        /* copy in the float_value data */
        numlen = sizeof(scalar_val->val.float_value);
        offset = reserve_from_buffer(buffer, numlen);
        *((float8 *)(buffer->data + offset)) = scalar_val->val.float_value;

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_TIMESTAMP:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_TIMESTAMP);

        offset = reserve_from_buffer(buffer, sizeof(int64));
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

	*gtentry = GTENTRY_IS_DYNAMIC | (sizeof(int64) + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_TIMESTAMPTZ:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_TIMESTAMPTZ);

        /* copy in the int_value data */
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_DATE:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_DATE);

        /* copy in the date data */
        numlen = sizeof(int32);
        offset = reserve_from_buffer(buffer, numlen);
        *((int32 *)(buffer->data + offset)) = scalar_val->val.date;

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
	break;
    case DYNAMIC_TIME:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_TIME);
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_TIMETZ:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_TIMETZ);

        /* copy in the timetz data */
        numlen = sizeof(TimeTzADT);
        offset = reserve_from_buffer(buffer, numlen);
        *((TimeADT *)(buffer->data + offset)) = scalar_val->val.timetz.time;
        *((int32 *)(buffer->data + offset + sizeof(TimeADT))) = scalar_val->val.timetz.zone;

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_INTERVAL:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_INTERVAL);

        numlen = sizeof(TimeOffset) + (2 * sizeof(int32));
        offset = reserve_from_buffer(buffer, numlen);
        *((TimeOffset *)(buffer->data + offset)) = scalar_val->val.interval.time;

        *((int32 *)(buffer->data + offset + sizeof(TimeOffset))) = scalar_val->val.interval.day;
        *((int32 *)(buffer->data + offset + sizeof(TimeOffset) + sizeof(int32))) = scalar_val->val.interval.month;

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_INET:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_INET);

	numlen = sizeof(char) * 22;
        offset = reserve_from_buffer(buffer, numlen);
	memcpy(buffer->data + offset, &scalar_val->val.inet, numlen);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
	break;
    case DYNAMIC_CIDR:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_CIDR);

        numlen = sizeof(char) * 22;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.inet, numlen);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_MAC:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_MAC);

        numlen = sizeof(char) * 6;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.mac, numlen);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_MAC8:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_MAC8);

        numlen = sizeof(char) * 8;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.mac, numlen);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;

    case DYNAMIC_POINT:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_POINT);

        numlen = sizeof(Point);
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.box, sizeof(Point));

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_LSEG:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_LSEG);

        numlen = sizeof(LSEG);
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.box, sizeof(LSEG));

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_LINE:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_LINE);

        numlen = sizeof(LINE);
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.box, sizeof(LINE));

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_PATH:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_PATH);

        numlen = scalar_val->val.path->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.path, scalar_val->val.path->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
        break;
    case DYNAMIC_POLYGON:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_POLYGON);

        numlen = scalar_val->val.polygon->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.polygon, scalar_val->val.polygon->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_CIRCLE:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_CIRCLE);

        numlen = sizeof(CIRCLE);
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.circle, sizeof(CIRCLE));

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_BOX:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_BOX);

        numlen = sizeof(BOX);
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.box, sizeof(BOX));

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;

    case DYNAMIC_BYTEA:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_BYTEA);

        numlen = *((int *)(scalar_val->val.bytea)->vl_len_) / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.bytea, *((int *)(scalar_val->val.bytea)->vl_len_) / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_TSVECTOR:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_TSVECTOR);

        numlen = (scalar_val->val.tsvector)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.tsvector, (scalar_val->val.tsvector)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_TSQUERY:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_TSQUERY);

        numlen = (scalar_val->val.tsquery)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.tsquery, (scalar_val->val.tsquery)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_INT:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_INT);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_INT_MULTI:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_INT_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_NUM:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_NUM);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_NUM_MULTI:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_NUM_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_TS:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_TS);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_TS_MULTI:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_TS_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_TSTZ:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_TSTZ);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_TSTZ_MULTI:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_TSTZ_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_DATE:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_DATE);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    case DYNAMIC_RANGE_DATE_MULTI:
        padlen = ag_serialize_header(buffer, DYNA_HEADER_RANGE_DATE_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_DYNAMIC | (padlen + numlen + DYNA_HEADER_SIZE);
        break;
    default:
        return false;
    }
    return true;
}

/*
 * Function deserializes the data from the buffer pointed to by base_addr.
 * NOTE: This function writes to the error log and exits for any UNKNOWN
 * AGT_HEADER type.
 */
void ag_deserialize_extended_type(char *base_addr, uint32 offset, dynamic_value *result) {
    char *base = base_addr + INTALIGN(offset);
    DYNA_HEADER_TYPE agt_header = *((DYNA_HEADER_TYPE *)base);

    switch (agt_header)
    {
    case DYNA_HEADER_INTEGER:
        result->type = DYNAMIC_INTEGER;
        result->val.int_value = *((int64 *)(base + DYNA_HEADER_SIZE));
        break;

    case DYNA_HEADER_FLOAT:
        result->type = DYNAMIC_FLOAT;
        result->val.float_value = *((float8 *)(base + DYNA_HEADER_SIZE));
        break;
    case DYNA_HEADER_BYTEA:
        result->type = DYNAMIC_BYTEA;
        result->val.bytea = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_TIMESTAMP:
        result->type = DYNAMIC_TIMESTAMP;
        result->val.int_value = *((int64 *)(base + DYNA_HEADER_SIZE));
        break;
    case DYNA_HEADER_TIMESTAMPTZ:
        result->type = DYNAMIC_TIMESTAMPTZ;
        result->val.int_value = *((int64 *)(base + DYNA_HEADER_SIZE));
        break;
    case DYNA_HEADER_DATE:
        result->type = DYNAMIC_DATE;
        result->val.date = *((int32 *)(base + DYNA_HEADER_SIZE));
        break;
    case DYNA_HEADER_TIME:
        result->type = DYNAMIC_TIME;
        result->val.int_value = *((int64 *)(base + DYNA_HEADER_SIZE));
        break;
    case DYNA_HEADER_TIMETZ:
        result->type = DYNAMIC_TIMETZ;
        result->val.timetz.time = *((TimeADT*)(base + DYNA_HEADER_SIZE));
        result->val.timetz.zone = *((int32*)(base + DYNA_HEADER_SIZE + sizeof(TimeADT)));
        break;
    case DYNA_HEADER_INTERVAL:
        result->type = DYNAMIC_INTERVAL;
        result->val.interval.time =  *((TimeOffset *)(base + DYNA_HEADER_SIZE));
        result->val.interval.day =  *((int32 *)(base + DYNA_HEADER_SIZE + sizeof(TimeOffset)));
        result->val.interval.month =  *((int32 *)(base + DYNA_HEADER_SIZE + sizeof(TimeOffset) + sizeof(int32)));
        break;
    case DYNA_HEADER_INET:
	result->type = DYNAMIC_INET;
	memcpy(&result->val.inet, base + DYNA_HEADER_SIZE, sizeof(char) * 22);
        break;
    case DYNA_HEADER_CIDR:
        result->type = DYNAMIC_CIDR;
        memcpy(&result->val.inet, base + DYNA_HEADER_SIZE, sizeof(char) * 22);
        break;
    case DYNA_HEADER_MAC:
        result->type = DYNAMIC_MAC;
        memcpy(&result->val.mac, base + DYNA_HEADER_SIZE, sizeof(char) * 6);
        break;
    case DYNA_HEADER_MAC8:
        result->type = DYNAMIC_MAC8;
        memcpy(&result->val.mac, base + DYNA_HEADER_SIZE, sizeof(char) * 8);
        break;
    case DYNA_HEADER_POINT:
        result->type = DYNAMIC_POINT;
	    result->val.point = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_LSEG:
        result->type = DYNAMIC_LSEG;
	    result->val.lseg = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_LINE:
        result->type = DYNAMIC_LINE;
	    result->val.line = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_PATH:
        result->type = DYNAMIC_PATH;
	    result->val.path = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_POLYGON:
        result->type = DYNAMIC_POLYGON;
	    result->val.polygon = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_CIRCLE:
        result->type = DYNAMIC_CIRCLE;
	    result->val.circle = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_BOX:
        result->type = DYNAMIC_BOX;
	    result->val.box = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_TSVECTOR:
        result->type = DYNAMIC_TSVECTOR;
        result->val.range = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_TSQUERY:
        result->type = DYNAMIC_TSQUERY;
        result->val.tsquery = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_INT:
        result->type = DYNAMIC_RANGE_INT;
        result->val.range = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_INT_MULTI:
        result->type = DYNAMIC_RANGE_INT_MULTI;
        result->val.multirange = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_NUM:
        result->type = DYNAMIC_RANGE_NUM;
        result->val.range = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_NUM_MULTI:
        result->type = DYNAMIC_RANGE_NUM_MULTI;
        result->val.multirange = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_TS:
        result->type = DYNAMIC_RANGE_TS;
        result->val.range = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_TS_MULTI:
        result->type = DYNAMIC_RANGE_TS_MULTI;
        result->val.multirange = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_TSTZ:
        result->type = DYNAMIC_RANGE_TSTZ;
        result->val.range = (base + DYNA_HEADER_SIZE); 
        break;
    case DYNA_HEADER_RANGE_TSTZ_MULTI:
        result->type = DYNAMIC_RANGE_TSTZ_MULTI;
        result->val.multirange = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_DATE:
        result->type = DYNAMIC_RANGE_DATE;
        result->val.range = (base + DYNA_HEADER_SIZE);
        break;
    case DYNA_HEADER_RANGE_DATE_MULTI:
        result->type = DYNAMIC_RANGE_DATE_MULTI;
        result->val.multirange = (base + DYNA_HEADER_SIZE);
        break;
    default:
        elog(ERROR, "Invalid AGT header value.");
    }
}
