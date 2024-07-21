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

#include "access/hash.h"
#include "catalog/pg_collation.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "tsearch/ts_utils.h"

#include "utils/dynamic.h"
#include "utils/dynamic_ext.h"

/*
 * Maximum number of elements in an array (or key/value pairs in an object).
 * This is limited by two things: the size of the gtentry array must fit
 * in MaxAllocSize, and the number of elements (or pairs) must fit in the bits
 * reserved for that in the dynamic_container.header field.
 *
 * (The total size of an array's or object's elements is also limited by
 * GTENTRY_OFFLENMASK, but we're not concerned about that here.)
 */
#define DYNAMIC_MAX_ELEMS (Min(MaxAllocSize / sizeof(dynamic_value), GT_CMASK))
#define DYNAMIC_MAX_PAIRS (Min(MaxAllocSize / sizeof(dynamic_pair), GT_CMASK))

static void fill_dynamic_value(dynamic_container *container, int index,
                              char *base_addr, uint32 offset,
                              dynamic_value *result);
static bool equals_dynamic_scalar_value(const dynamic_value *a, const dynamic_value *b);
static dynamic *convert_to_dynamic(dynamic_value *val);
static void convert_dynamic_value(StringInfo buffer, gtentry *header, dynamic_value *val, int level);
static void convert_dynamic_array(StringInfo buffer, gtentry *pheader, dynamic_value *val, int level);
static void convert_dynamic_object(StringInfo buffer, gtentry *pheader, dynamic_value *val, int level);
static void convert_dynamic_scalar(StringInfo buffer, gtentry *entry, dynamic_value *scalar_val);
static void append_to_buffer(StringInfo buffer, const char *data, int len);
static void copy_to_buffer(StringInfo buffer, int offset, const char *data, int len);
static dynamic_iterator *iterator_from_container(dynamic_container *container, dynamic_iterator *parent);
static dynamic_iterator *free_and_get_parent(dynamic_iterator *it);
static dynamic_parse_state *push_state(dynamic_parse_state **pstate);
static void append_key(dynamic_parse_state *pstate, dynamic_value *string);
static void append_value(dynamic_parse_state *pstate, dynamic_value *scalar_val);
static void append_element(dynamic_parse_state *pstate, dynamic_value *scalar_val);
static int length_compare_dynamic_string_value(const void *a, const void *b);
static int length_compare_dynamic_pair(const void *a, const void *b, void *binequal);
static dynamic_value *push_dynamic_value_scalar(dynamic_parse_state **pstate,
                                              dynamic_iterator_token seq,
                                              dynamic_value *scalar_val);
static int compare_two_floats_orderability(float8 lhs, float8 rhs);
static int get_type_sort_priority(enum dynamic_value_type type);
static int compare_range_internal(dynamic_value *a, dynamic_value *b);
static int CompareTSQ(TSQuery a, TSQuery b);
static int silly_cmp_tsvector(const TSVector a, const TSVector b);

bool is_numeric_result(dynamic_value *lhs, dynamic_value *rhs)
{
    if (((lhs->type == DYNAMIC_NUMERIC || rhs->type == DYNAMIC_NUMERIC) &&
         (lhs->type == DYNAMIC_INTEGER || lhs->type == DYNAMIC_FLOAT ||
          rhs->type == DYNAMIC_INTEGER || rhs->type == DYNAMIC_FLOAT )) ||
        (lhs->type == DYNAMIC_NUMERIC && rhs->type == DYNAMIC_NUMERIC))
        return true;
    return false;
}

#include "common/int128.h"

int
timetz_cmp_internal(TimeTzADT *time1, TimeTzADT *time2) {
    TimeOffset t1, t2;

    // Primary sort is by true (GMT-equivalent) time
    t1 = time1->time + (time1->zone * USECS_PER_SEC);
    t2 = time2->time + (time2->zone * USECS_PER_SEC);

    if (t1 > t2)
        return 1;
    if (t1 < t2)
        return -1;

    /*
     * If same GMT time, sort by timezone; we only want to say that two
     * timetz's are equal if both the time and zone parts are equal.
     */
    if (time1->zone > time2->zone)
        return 1;
    if (time1->zone < time2->zone)
        return -1;

    return 0;
}

static inline INT128
interval_cmp_value(const Interval *interval)
{
    INT128 span;
    int64 dayfraction;
    int64 days;

    /*
     * Separate time field into days and dayfraction, then add the month and
     * day fields to the days part.  We cannot overflow int64 days here.
     */
    dayfraction = interval->time % USECS_PER_DAY;
    days = interval->time / USECS_PER_DAY;
    days += interval->month * INT64CONST(30);
    days += interval->day;

    // Widen dayfraction to 128 bits
    span = int64_to_int128(dayfraction);

    // Scale up days to microseconds, forming a 128-bit product
    int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);

    return span;
}

int
interval_cmp_internal(Interval *interval1, Interval *interval2)
{
    INT128 span1 = interval_cmp_value(interval1);
    INT128 span2 = interval_cmp_value(interval2);

    return int128_compare(span1, span2);
}

/*
 * Turn an in-memory dynamic_value into an dynamic for on-disk storage.
 *
 * There isn't an dynamic_to_dynamic_value(), because generally we find it more
 * convenient to directly iterate through the dynamic representation and only
 * really convert nested scalar values.  dynamic_iterator_next() does this, so
 * that clients of the iteration code don't have to directly deal with the
 * binary representation (dynamic_deep_contains() is a notable exception,
 * although all exceptions are internal to this module).  In general, functions
 * that accept an dynamic_value argument are concerned with the manipulation of
 * scalar values, or simple containers of scalar values, where it would be
 * inconvenient to deal with a great amount of other state.
 */
dynamic *dynamic_value_to_dynamic(dynamic_value *val)
{
    dynamic *out;

    if (IS_A_DYNAMIC_SCALAR(val))
    {
        // Scalar value 
        dynamic_parse_state *pstate = NULL;
        dynamic_value *res;
        dynamic_value scalar_array;

        scalar_array.type = DYNAMIC_ARRAY;
        scalar_array.val.array.raw_scalar = true;
        scalar_array.val.array.num_elems = 1;

        push_dynamic_value(&pstate, WGT_BEGIN_ARRAY, &scalar_array);
        push_dynamic_value(&pstate, WGT_ELEM, val);
        res = push_dynamic_value(&pstate, WGT_END_ARRAY, NULL);

        out = convert_to_dynamic(res);
    }
    else if (val->type == DYNAMIC_OBJECT || val->type == DYNAMIC_ARRAY)
    {
        out = convert_to_dynamic(val);
    }
    else
    {
        Assert(val->type == DYNAMIC_BINARY);
        out = palloc(VARHDRSZ + val->val.binary.len);
        SET_VARSIZE(out, VARHDRSZ + val->val.binary.len);
        memcpy(VARDATA(out), val->val.binary.data, val->val.binary.len);
    }

    return out;
}

/*
 * Get the offset of the variable-length portion of an dynamic node within
 * the variable-length-data part of its container.  The node is identified
 * by index within the container's gtentry array.
 */
uint32 get_dynamic_offset(const dynamic_container *agtc, int index)
{
    uint32 offset = 0;
    int i;

    /*
     * Start offset of this entry is equal to the end offset of the previous
     * entry.  Walk backwards to the most recent entry stored as an end
     * offset, returning that offset plus any lengths in between.
     */
    for (i = index - 1; i >= 0; i--)
    {
        offset += GTE_OFFLENFLD(agtc->children[i]);
        if (GTE_HAS_OFF(agtc->children[i]))
            break;
    }

    return offset;
}

/*
 * Get the length of the variable-length portion of an dynamic node.
 * The node is identified by index within the container's gtentry array.
 */
uint32 get_dynamic_length(const dynamic_container *agtc, int index)
{
    uint32 off;
    uint32 len;

    /*
     * If the length is stored directly in the gtentry, just return it.
     * Otherwise, get the begin offset of the entry, and subtract that from
     * the stored end+1 offset.
     */
    if (GTE_HAS_OFF(agtc->children[index]))
    {
        off = get_dynamic_offset(agtc, index);
        len = GTE_OFFLENFLD(agtc->children[index]) - off;
    }
    else
    {
        len = GTE_OFFLENFLD(agtc->children[index]);
    }

    return len;
}

/*
 * Helper function to generate the sort priorty of a type. Larger
 * numbers have higher priority.
 */
static int get_type_sort_priority(enum dynamic_value_type type)
{
    if (type == DYNAMIC_OBJECT)
        return 0;
    if (type == DYNAMIC_ARRAY)
        return 1;
    if (type == DYNAMIC_STRING)
        return 2;
    if (type == DYNAMIC_BOOL)
        return 3;
    if (type == DYNAMIC_NUMERIC || type == DYNAMIC_INTEGER || type == DYNAMIC_FLOAT)
        return 4;
    if (type == DYNAMIC_TIMESTAMP || type == DYNAMIC_TIMESTAMPTZ)
	return 5;
    if (type == DYNAMIC_DATE)
	return 6;
    if (type == DYNAMIC_TIME || DYNAMIC_TIMETZ)
	 return 7;
    if (type == DYNAMIC_INTERVAL)
	 return 8;  
    if (type == DYNAMIC_INET)
	 return 9;
    if (type == DYNAMIC_CIDR)
	 return 10; 
    if (type == DYNAMIC_NULL)
        return 11;
    return -1;
}

/*
 * BT comparator worker function.  Returns an integer less than, equal to, or
 * greater than zero, indicating whether a is less than, equal to, or greater
 * than b.  Consistent with the requirements for a B-Tree operator class
 *
 * Strings are compared lexically, in contrast with other places where we use a
 * much simpler comparator logic for searching through Strings.  Since this is
 * called from B-Tree support function 1, we're careful about not leaking
 * memory here.
 */
int compare_dynamic_containers_orderability(dynamic_container *a, dynamic_container *b)
{
    dynamic_iterator *ita;
    dynamic_iterator *itb;
    int res = 0;

    ita = dynamic_iterator_init(a);
    itb = dynamic_iterator_init(b);

    do
    {
        dynamic_value va;
        dynamic_value vb;
        dynamic_iterator_token ra;
        dynamic_iterator_token rb;

        ra = dynamic_iterator_next(&ita, &va, false);
        rb = dynamic_iterator_next(&itb, &vb, false);

        if (ra == rb)
        {
            if (ra == WGT_DONE)
            {
                // Decisively equal 
                break;
            }

            if (ra == WGT_END_ARRAY || ra == WGT_END_OBJECT)
            {
                /*
                 * There is no array or object to compare at this stage of
                 * processing.  DYNAMIC_ARRAY/DYNAMIC_OBJECT values are compared
                 * initially, at the WGT_BEGIN_ARRAY and WGT_BEGIN_OBJECT
                 * tokens.
                 */
                continue;
            }

            if ((va.type == vb.type) ||
                ((va.type == DYNAMIC_INTEGER || va.type == DYNAMIC_FLOAT || va.type == DYNAMIC_NUMERIC ||
		  va.type == DYNAMIC_TIMESTAMP || va.type == DYNAMIC_DATE || va.type == DYNAMIC_TIMESTAMPTZ ||
		  va.type == DYNAMIC_TIMETZ || va.type == DYNAMIC_TIME || va.type == DYNAMIC_DATE ||
		  va.type == DYNAMIC_INET || va.type == DYNAMIC_CIDR || va.type == DYNAMIC_RANGE_INT ||
		  va.type == DYNAMIC_RANGE_NUM || va.type == DYNAMIC_RANGE_TS || va.type == DYNAMIC_RANGE_TSTZ ||
		  va.type == DYNAMIC_RANGE_DATE || va.type == DYNAMIC_TSQUERY) &&
                 (vb.type == DYNAMIC_INTEGER || vb.type == DYNAMIC_FLOAT || vb.type == DYNAMIC_NUMERIC || 
		  vb.type == DYNAMIC_TIMESTAMP || vb.type == DYNAMIC_DATE || vb.type == DYNAMIC_TIMESTAMPTZ ||
                  vb.type == DYNAMIC_TIMETZ || vb.type == DYNAMIC_TIME || vb.type == DYNAMIC_DATE ||
		  vb.type == DYNAMIC_INET || va.type == DYNAMIC_CIDR || vb.type == DYNAMIC_RANGE_INT ||
		  vb.type == DYNAMIC_RANGE_NUM || vb.type == DYNAMIC_RANGE_TS || vb.type == DYNAMIC_RANGE_TSTZ ||
		  vb.type == DYNAMIC_RANGE_DATE || vb.type == DYNAMIC_TSQUERY)))
            {
                switch (va.type)
                {
                case DYNAMIC_STRING:
                case DYNAMIC_NULL:
                case DYNAMIC_NUMERIC:
                case DYNAMIC_BOOL:
                case DYNAMIC_INTEGER:
                case DYNAMIC_FLOAT:
		case DYNAMIC_TIMESTAMP:
		case DYNAMIC_TIMESTAMPTZ:
		case DYNAMIC_DATE:
		case DYNAMIC_TIME:
		case DYNAMIC_TIMETZ:
		case DYNAMIC_INTERVAL:
		case DYNAMIC_INET:
		case DYNAMIC_CIDR:
		case DYNAMIC_RANGE_INT:
                case DYNAMIC_RANGE_NUM:
		case DYNAMIC_RANGE_DATE:
		case DYNAMIC_RANGE_TS:
                case DYNAMIC_RANGE_TSTZ:
		case DYNAMIC_TSQUERY:
		case DYNAMIC_TSVECTOR:
                    res = compare_dynamic_scalar_values(&va, &vb);
                    break;
                case DYNAMIC_ARRAY:
                    /*
                     * This could be a "raw scalar" pseudo array.  That's
                     * a special case here though, since we still want the
                     * general type-based comparisons to apply, and as far
                     * as we're concerned a pseudo array is just a scalar.
                     */
                    if (va.val.array.raw_scalar != vb.val.array.raw_scalar)
                    {
                        if (va.val.array.raw_scalar)
                        {
                            // advance iterator ita and get contained type 
                            ra = dynamic_iterator_next(&ita, &va, false);
                            res = (get_type_sort_priority(va.type) < get_type_sort_priority(vb.type)) ?  -1 : 1;
                        }
                        else
                        {
                            // advance iterator itb and get contained type 
                            rb = dynamic_iterator_next(&itb, &vb, false);
                            res = (get_type_sort_priority(va.type) < get_type_sort_priority(vb.type)) ?  -1 : 1;
                        }
                    }
                    break;
                case DYNAMIC_OBJECT:
                    break;
                case DYNAMIC_BINARY:
                    ereport(ERROR, (errmsg("unexpected DYNAMIC_BINARY value")));
                default:
		    ereport(ERROR, (errmsg("unexpected dynamic for comparison")));
		}
            }
            else
            {
                // Type-defined order 
                res = (get_type_sort_priority(va.type) < get_type_sort_priority(vb.type)) ?  -1 : 1;
            }
        }
        else
        {
            /*
             * It's safe to assume that the types differed, and that the va
             * and vb values passed were set.
             *
             * If the two values were of the same container type, then there'd
             * have been a chance to observe the variation in the number of
             * elements/pairs (when processing WGT_BEGIN_OBJECT, say). They're
             * either two heterogeneously-typed containers, or a container and
             * some scalar type.
             */

            /*
             * Check for the premature array or object end.
             * If left side is shorter, less than.
             */
            if (ra == WGT_END_ARRAY || ra == WGT_END_OBJECT)
            {
                res = -1;
                break;
            }
            // If right side is shorter, greater than 
            if (rb == WGT_END_ARRAY || rb == WGT_END_OBJECT)
            {
                res = 1;
                break;
            }

            Assert(va.type != vb.type);
            Assert(va.type != DYNAMIC_BINARY);
            Assert(vb.type != DYNAMIC_BINARY);
            // Type-defined order 
            res = (get_type_sort_priority(va.type) < get_type_sort_priority(vb.type)) ?  -1 : 1;
        }
    } while (res == 0);

    while (ita != NULL)
    {
        dynamic_iterator *i = ita->parent;

        pfree(ita);
        ita = i;
    }
    while (itb != NULL)
    {
        dynamic_iterator *i = itb->parent;

        pfree(itb);
        itb = i;
    }

    return res;
}

/*
 * Find value in object (i.e. the "value" part of some key/value pair in an
 * object), or find a matching element if we're looking through an array.  Do
 * so on the basis of equality of the object keys only, or alternatively
 * element values only, with a caller-supplied value "key".  The "flags"
 * argument allows the caller to specify which container types are of interest.
 *
 * This exported utility function exists to facilitate various cases concerned
 * with "containment".  If asked to look through an object, the caller had
 * better pass an dynamic String, because their keys can only be strings.
 * Otherwise, for an array, any type of dynamic_value will do.
 *
 * In order to proceed with the search, it is necessary for callers to have
 * both specified an interest in exactly one particular container type with an
 * appropriate flag, as well as having the pointed-to dynamic container be of
 * one of those same container types at the top level. (Actually, we just do
 * whichever makes sense to save callers the trouble of figuring it out - at
 * most one can make sense, because the container either points to an array
 * (possibly a "raw scalar" pseudo array) or an object.)
 *
 * Note that we can return an DYNAMIC_BINARY dynamic_value if this is called on an
 * object, but we never do so on an array.  If the caller asks to look through
 * a container type that is not of the type pointed to by the container,
 * immediately fall through and return NULL.  If we cannot find the value,
 * return NULL.  Otherwise, return palloc()'d copy of value.
 */
dynamic_value *find_dynamic_value_from_container(dynamic_container *container,
                                               uint32 flags, const dynamic_value *key)
{
    gtentry *children = container->children;
    int count = DYNAMIC_CONTAINER_SIZE(container);
    dynamic_value *result;

    Assert((flags & ~(GT_FARRAY | GT_FOBJECT)) == 0);

    // Quick out without a palloc cycle if object/array is empty 
    if (count <= 0)
    {
        return NULL;
    }

    result = palloc(sizeof(dynamic_value));

    if ((flags & GT_FARRAY) && DYNAMIC_CONTAINER_IS_ARRAY(container))
    {
        char *base_addr = (char *)(children + count);
        uint32 offset = 0;
        int i;

        for (i = 0; i < count; i++)
        {
            fill_dynamic_value(container, i, base_addr, offset, result);

            if (key->type == result->type)
            {
                if (equals_dynamic_scalar_value(key, result))
                    return result;
            }

            GTE_ADVANCE_OFFSET(offset, children[i]);
        }
    }
    else if ((flags & GT_FOBJECT) && DYNAMIC_CONTAINER_IS_OBJECT(container))
    {
        // Since this is an object, account for *Pairs* of AGTentrys 
        char *base_addr = (char *)(children + count * 2);
        uint32 stop_low = 0;
        uint32 stop_high = count;

        // Object key passed by caller must be a string 
        Assert(key->type == DYNAMIC_STRING);

        // Binary search on object/pair keys *only* 
        while (stop_low < stop_high)
        {
            uint32 stop_middle;
            int difference;
            dynamic_value candidate;

            stop_middle = stop_low + (stop_high - stop_low) / 2;

            candidate.type = DYNAMIC_STRING;
            candidate.val.string.val =
                base_addr + get_dynamic_offset(container, stop_middle);
            candidate.val.string.len = get_dynamic_length(container,
                                                         stop_middle);

            difference = length_compare_dynamic_string_value(&candidate, key);

            if (difference == 0)
            {
                // Found our key, return corresponding value 
                int index = stop_middle + count;

                fill_dynamic_value(container, index, base_addr,
                                  get_dynamic_offset(container, index), result);

                return result;
            }
            else
            {
                if (difference < 0)
                    stop_low = stop_middle + 1;
                else
                    stop_high = stop_middle;
            }
        }
    }

    // Not found 
    pfree(result);
    return NULL;
}

/*
 * Get i-th value of an dynamic array.
 *
 * Returns palloc()'d copy of the value, or NULL if it does not exist.
 */
dynamic_value *get_ith_dynamic_value_from_container(dynamic_container *container,
                                                  uint32 i)
{
    dynamic_value *result;
    char *base_addr;
    uint32 nelements;

    if (!DYNAMIC_CONTAINER_IS_ARRAY(container))
        ereport(ERROR, (errmsg("container is not an dynamic array")));

    nelements = DYNAMIC_CONTAINER_SIZE(container);
    base_addr = (char *)&container->children[nelements];

    if (i >= nelements)
        return NULL;

    result = palloc(sizeof(dynamic_value));

    fill_dynamic_value(container, i, base_addr, get_dynamic_offset(container, i),
                      result);

    return result;
}

/*
 * A helper function to fill in an dynamic_value to represent an element of an
 * array, or a key or value of an object.
 *
 * The node's gtentry is at container->children[index], and its variable-length
 * data is at base_addr + offset.  We make the caller determine the offset
 * since in many cases the caller can amortize that work across multiple
 * children.  When it can't, it can just call get_dynamic_offset().
 *
 * A nested array or object will be returned as DYNAMIC_BINARY, ie. it won't be
 * expanded.
 */
static void fill_dynamic_value(dynamic_container *container, int index,
                              char *base_addr, uint32 offset,
                              dynamic_value *result)
{
    gtentry entry = container->children[index];

    if (GTE_IS_NULL(entry))
    {
        result->type = DYNAMIC_NULL;
    }
    else if (GTE_IS_STRING(entry))
    {
        char *string_val;
        int string_len;

        result->type = DYNAMIC_STRING;
        // get the position and length of the string 
        string_val = base_addr + offset;
        string_len = get_dynamic_length(container, index);
        // we need to do a deep copy of the string value 
        result->val.string.val = pnstrdup(string_val, string_len);
        result->val.string.len = string_len;
        Assert(result->val.string.len >= 0);
    }
    else if (GTE_IS_NUMERIC(entry))
    {
        Numeric numeric;
        Numeric numeric_copy;

        result->type = DYNAMIC_NUMERIC;
        // we need to do a deep copy here 
        numeric = (Numeric)(base_addr + INTALIGN(offset));
        numeric_copy = (Numeric) palloc(VARSIZE(numeric));
        memcpy(numeric_copy, numeric, VARSIZE(numeric));
        result->val.numeric = numeric_copy;
    }
    /*
     * If this is an dynamic.
     * This is needed because we allow the original jsonb type to be
     * passed in.
     */
    else if (GTE_IS_DYNAMIC(entry))
    {
        ag_deserialize_extended_type(base_addr, offset, result);
    }
    else if (GTE_IS_BOOL_TRUE(entry))
    {
        result->type = DYNAMIC_BOOL;
        result->val.boolean = true;
    }
    else if (GTE_IS_BOOL_FALSE(entry))
    {
        result->type = DYNAMIC_BOOL;
        result->val.boolean = false;
    }
    else
    {
        Assert(GTE_IS_CONTAINER(entry));
        result->type = DYNAMIC_BINARY;
        // Remove alignment padding from data pointer and length 
        result->val.binary.data =
            (dynamic_container *)(base_addr + INTALIGN(offset));
        result->val.binary.len = get_dynamic_length(container, index) -
                                 (INTALIGN(offset) - offset);
    }
}

/*
 * Push dynamic_value into dynamic_parse_state.
 *
 * Used when parsing dynamic tokens to form dynamic, or when converting an
 * in-memory dynamic_value to an dynamic.
 *
 * Initial state of *dynamic_parse_state is NULL, since it'll be allocated here
 * originally (caller will get dynamic_parse_state back by reference).
 *
 * Only sequential tokens pertaining to non-container types should pass an
 * dynamic_value.  There is one exception -- WGT_BEGIN_ARRAY callers may pass a
 * "raw scalar" pseudo array to append it - the actual scalar should be passed
 * next and it will be added as the only member of the array.
 *
 * Values of type DYNAMIC_BINARY, which are rolled up arrays and objects,
 * are unpacked before being added to the result.
 */
dynamic_value *push_dynamic_value(dynamic_parse_state **pstate,
                                dynamic_iterator_token seq,
                                dynamic_value *agtval)
{
    dynamic_iterator *it;
    dynamic_value *res = NULL;
    dynamic_value v;
    dynamic_iterator_token tok;

    if (!agtval || (seq != WGT_ELEM && seq != WGT_VALUE) ||
        (agtval->type != DYNAMIC_BINARY))
    {
        // drop through 
        return push_dynamic_value_scalar(pstate, seq, agtval);
    }

    // unpack the binary and add each piece to the pstate 
    it = dynamic_iterator_init(agtval->val.binary.data);
    while ((tok = dynamic_iterator_next(&it, &v, false)) != WGT_DONE)
    {
        res = push_dynamic_value_scalar(pstate, tok,
                                       tok < WGT_BEGIN_ARRAY ? &v : NULL);
    }

    return res;
}

/*
 * Do the actual pushing, with only scalar or pseudo-scalar-array values
 * accepted.
 */
static dynamic_value *push_dynamic_value_scalar(dynamic_parse_state **pstate,
                                              dynamic_iterator_token seq,
                                              dynamic_value *scalar_val)
{
    dynamic_value *result = NULL;

    switch (seq)
    {
    case WGT_BEGIN_ARRAY:
        Assert(!scalar_val || scalar_val->val.array.raw_scalar);
        *pstate = push_state(pstate);
        result = &(*pstate)->cont_val;
        (*pstate)->cont_val.type = DYNAMIC_ARRAY;
        (*pstate)->cont_val.val.array.num_elems = 0;
        (*pstate)->cont_val.val.array.raw_scalar =
            (scalar_val && scalar_val->val.array.raw_scalar);
        if (scalar_val && scalar_val->val.array.num_elems > 0)
        {
            // Assume that this array is still really a scalar 
            Assert(scalar_val->type == DYNAMIC_ARRAY);
            (*pstate)->size = scalar_val->val.array.num_elems;
        }
        else
        {
            (*pstate)->size = 4;
        }
        (*pstate)->cont_val.val.array.elems =
            palloc(sizeof(dynamic_value) * (*pstate)->size);
        (*pstate)->last_updated_value = NULL;
        break;
    case WGT_BEGIN_OBJECT:
        Assert(!scalar_val);
        *pstate = push_state(pstate);
        result = &(*pstate)->cont_val;
        (*pstate)->cont_val.type = DYNAMIC_OBJECT;
        (*pstate)->cont_val.val.object.num_pairs = 0;
        (*pstate)->size = 4;
        (*pstate)->cont_val.val.object.pairs =
            palloc(sizeof(dynamic_pair) * (*pstate)->size);
        (*pstate)->last_updated_value = NULL;
        break;
    case WGT_KEY:
        Assert(scalar_val->type == DYNAMIC_STRING);
        append_key(*pstate, scalar_val);
        break;
    case WGT_VALUE:
        Assert(IS_A_DYNAMIC_SCALAR(scalar_val));
        append_value(*pstate, scalar_val);
        break;
    case WGT_ELEM:
        Assert(IS_A_DYNAMIC_SCALAR(scalar_val));
        append_element(*pstate, scalar_val);
        break;
    case WGT_END_OBJECT:
        uniqueify_dynamic_object(&(*pstate)->cont_val);
        // fall through! 
    case WGT_END_ARRAY:
        Assert(!scalar_val);
        result = &(*pstate)->cont_val;

        /*
         * Pop queue and push current array/object as value in parent
         * array/object
         */
        *pstate = (*pstate)->next;
        if (*pstate)
        {
            switch ((*pstate)->cont_val.type)
            {
            case DYNAMIC_ARRAY:
                append_element(*pstate, result);
                break;
            case DYNAMIC_OBJECT:
                append_value(*pstate, result);
                break;
            default:
                ereport(ERROR, (errmsg("invalid dynamic container type %d",
                                       (*pstate)->cont_val.type)));
            }
        }
        break;
    default:
        ereport(ERROR,
                (errmsg("unrecognized dynamic sequential processing token")));
    }

    return result;
}

/*
 * push_dynamic_value() worker:  Iteration-like forming of dynamic
 */
static dynamic_parse_state *push_state(dynamic_parse_state **pstate)
{
    dynamic_parse_state *ns = palloc(sizeof(dynamic_parse_state));

    ns->next = *pstate;
    return ns;
}

/*
 * push_dynamic_value() worker:  Append a pair key to state when generating
 *                              dynamic
 */
static void append_key(dynamic_parse_state *pstate, dynamic_value *string)
{
    dynamic_value *object = &pstate->cont_val;

    Assert(object->type == DYNAMIC_OBJECT);
    Assert(string->type == DYNAMIC_STRING);

    if (object->val.object.num_pairs >= DYNAMIC_MAX_PAIRS)
        ereport(
            ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg(
                 "number of dynamic object pairs exceeds the maximum allowed (%zu)",
                 DYNAMIC_MAX_PAIRS)));

    if (object->val.object.num_pairs >= pstate->size)
    {
        pstate->size *= 2;
        object->val.object.pairs = repalloc(
            object->val.object.pairs, sizeof(dynamic_pair) * pstate->size);
    }

    object->val.object.pairs[object->val.object.num_pairs].key = *string;
    object->val.object.pairs[object->val.object.num_pairs].order =
        object->val.object.num_pairs;
}

/*
 * push_dynamic_value() worker:  Append a pair value to state when generating an
 *                              dynamic
 */
static void append_value(dynamic_parse_state *pstate, dynamic_value *scalar_val)
{
    dynamic_value *object = &pstate->cont_val;

    Assert(object->type == DYNAMIC_OBJECT);

    object->val.object.pairs[object->val.object.num_pairs].value = *scalar_val;

    pstate->last_updated_value =
        &object->val.object.pairs[object->val.object.num_pairs++].value;
}

/*
 * push_dynamic_value() worker:  Append an element to state when generating an
 *                              dynamic
 */
static void append_element(dynamic_parse_state *pstate,
                           dynamic_value *scalar_val)
{
    dynamic_value *array = &pstate->cont_val;

    Assert(array->type == DYNAMIC_ARRAY);

    if (array->val.array.num_elems >= DYNAMIC_MAX_ELEMS)
        ereport(
            ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg(
                 "number of dynamic array elements exceeds the maximum allowed (%zu)",
                 DYNAMIC_MAX_ELEMS)));

    if (array->val.array.num_elems >= pstate->size)
    {
        pstate->size *= 2;
        array->val.array.elems = repalloc(array->val.array.elems,
                                          sizeof(dynamic_value) * pstate->size);
    }

    array->val.array.elems[array->val.array.num_elems] = *scalar_val;
    pstate->last_updated_value =
        &array->val.array.elems[array->val.array.num_elems++];
}

/*
 * Given an dynamic_container, expand to dynamic_iterator to iterate over items
 * fully expanded to in-memory representation for manipulation.
 *
 * See dynamic_iterator_next() for notes on memory management.
 */
dynamic_iterator *dynamic_iterator_init(dynamic_container *container)
{
    return iterator_from_container(container, NULL);
}

/*
 * Get next dynamic_value while iterating
 *
 * Caller should initially pass their own, original iterator.  They may get
 * back a child iterator palloc()'d here instead.  The function can be relied
 * on to free those child iterators, lest the memory allocated for highly
 * nested objects become unreasonable, but only if callers don't end iteration
 * early (by breaking upon having found something in a search, for example).
 *
 * Callers in such a scenario, that are particularly sensitive to leaking
 * memory in a long-lived context may walk the ancestral tree from the final
 * iterator we left them with to its oldest ancestor, pfree()ing as they go.
 * They do not have to free any other memory previously allocated for iterators
 * but not accessible as direct ancestors of the iterator they're last passed
 * back.
 *
 * Returns "dynamic sequential processing" token value.  Iterator "state"
 * reflects the current stage of the process in a less granular fashion, and is
 * mostly used here to track things internally with respect to particular
 * iterators.
 *
 * Clients of this function should not have to handle any DYNAMIC_BINARY values
 * (since recursive calls will deal with this), provided skip_nested is false.
 * It is our job to expand the DYNAMIC_BINARY representation without bothering
 * them with it.  However, clients should not take it upon themselves to touch
 * array or Object element/pair buffers, since their element/pair pointers are
 * garbage.  Also, *val will not be set when returning WGT_END_ARRAY or
 * WGT_END_OBJECT, on the assumption that it's only useful to access values
 * when recursing in.
 */
dynamic_iterator_token dynamic_iterator_next(dynamic_iterator **it,
                                           dynamic_value *val, bool skip_nested)
{
    if (*it == NULL)
        return WGT_DONE;

    /*
     * When stepping into a nested container, we jump back here to start
     * processing the child. We will not recurse further in one call, because
     * processing the child will always begin in GTI_ARRAY_START or
     * GTI_OBJECT_START state.
     */
recurse:
    switch ((*it)->state)
    {
    case GTI_ARRAY_START:
        // Set v to array on first array call 
        val->type = DYNAMIC_ARRAY;
        val->val.array.num_elems = (*it)->num_elems;

        /*
         * v->val.array.elems is not actually set, because we aren't doing
         * a full conversion
         */
        val->val.array.raw_scalar = (*it)->is_scalar;
        (*it)->curr_index = 0;
        (*it)->curr_data_offset = 0;
        (*it)->curr_value_offset = 0; // not actually used 
        // Set state for next call 
        (*it)->state = GTI_ARRAY_ELEM;
        return WGT_BEGIN_ARRAY;

    case GTI_ARRAY_ELEM:
        if ((*it)->curr_index >= (*it)->num_elems)
        {
            /*
             * All elements within array already processed.  Report this
             * to caller, and give it back original parent iterator (which
             * independently tracks iteration progress at its level of
             * nesting).
             */
            *it = free_and_get_parent(*it);
            return WGT_END_ARRAY;
        }

        fill_dynamic_value((*it)->container, (*it)->curr_index,
                          (*it)->data_proper, (*it)->curr_data_offset, val);

        GTE_ADVANCE_OFFSET((*it)->curr_data_offset,
                            (*it)->children[(*it)->curr_index]);
        (*it)->curr_index++;

        if (!IS_A_DYNAMIC_SCALAR(val) && !skip_nested)
        {
            // Recurse into container. 
            *it = iterator_from_container(val->val.binary.data, *it);
            goto recurse;
        }
        else
        {
            /*
             * Scalar item in array, or a container and caller didn't want
             * us to recurse into it.
             */
            return WGT_ELEM;
        }

    case GTI_OBJECT_START:
        // Set v to object on first object call 
        val->type = DYNAMIC_OBJECT;
        val->val.object.num_pairs = (*it)->num_elems;

        /*
         * v->val.object.pairs is not actually set, because we aren't
         * doing a full conversion
         */
        (*it)->curr_index = 0;
        (*it)->curr_data_offset = 0;
        (*it)->curr_value_offset = get_dynamic_offset((*it)->container,
                                                     (*it)->num_elems);
        // Set state for next call 
        (*it)->state = GTI_OBJECT_KEY;
        return WGT_BEGIN_OBJECT;

    case GTI_OBJECT_KEY:
        if ((*it)->curr_index >= (*it)->num_elems)
        {
            /*
             * All pairs within object already processed.  Report this to
             * caller, and give it back original containing iterator
             * (which independently tracks iteration progress at its level
             * of nesting).
             */
            *it = free_and_get_parent(*it);
            return WGT_END_OBJECT;
        }
        else
        {
            // Return key of a key/value pair.  
            fill_dynamic_value((*it)->container, (*it)->curr_index,
                              (*it)->data_proper, (*it)->curr_data_offset,
                              val);
            if (val->type != DYNAMIC_STRING)
                ereport(ERROR,
                        (errmsg("unexpected dynamic type as object key %d",
                                val->type)));

            // Set state for next call 
            (*it)->state = GTI_OBJECT_VALUE;
            return WGT_KEY;
        }

    case GTI_OBJECT_VALUE:
        // Set state for next call 
        (*it)->state = GTI_OBJECT_KEY;

        fill_dynamic_value((*it)->container,
                          (*it)->curr_index + (*it)->num_elems,
                          (*it)->data_proper, (*it)->curr_value_offset, val);

        GTE_ADVANCE_OFFSET((*it)->curr_data_offset,
                            (*it)->children[(*it)->curr_index]);
        GTE_ADVANCE_OFFSET(
            (*it)->curr_value_offset,
            (*it)->children[(*it)->curr_index + (*it)->num_elems]);
        (*it)->curr_index++;

        /*
         * Value may be a container, in which case we recurse with new,
         * child iterator (unless the caller asked not to, by passing
         * skip_nested).
         */
        if (!IS_A_DYNAMIC_SCALAR(val) && !skip_nested)
        {
            *it = iterator_from_container(val->val.binary.data, *it);
            goto recurse;
        }
        else
        {
            return WGT_VALUE;
        }
    }

    ereport(ERROR, (errmsg("invalid iterator state %d", (*it)->state)));
    return -1;
}

/*
 * Initialize an iterator for iterating all elements in a container.
 */
static dynamic_iterator *iterator_from_container(dynamic_container *container,
                                                dynamic_iterator *parent)
{
    dynamic_iterator *it;

    it = palloc0(sizeof(dynamic_iterator));
    it->container = container;
    it->parent = parent;
    it->num_elems = DYNAMIC_CONTAINER_SIZE(container);

    // Array starts just after header 
    it->children = container->children;

    switch (container->header & (GT_FARRAY | GT_FOBJECT))
    {
    case GT_FARRAY:
        it->data_proper = (char *)it->children +
                          it->num_elems * sizeof(gtentry);
        it->is_scalar = DYNAMIC_CONTAINER_IS_SCALAR(container);
        // This is either a "raw scalar", or an array 
        Assert(!it->is_scalar || it->num_elems == 1);

        it->state = GTI_ARRAY_START;
        break;

    case GT_FOBJECT:
        it->data_proper = (char *)it->children +
                          it->num_elems * sizeof(gtentry) * 2;
        it->state = GTI_OBJECT_START;
        break;
    default:
        ereport(ERROR,
                (errmsg("unknown type of dynamic container %d",
                        container->header & (GT_FARRAY | GT_FOBJECT))));
    }

    return it;
}

/*
 * dynamic_iterator_next() worker: Return parent, while freeing memory for
 *                                current iterator
 */
static dynamic_iterator *free_and_get_parent(dynamic_iterator *it)
{
    dynamic_iterator *v = it->parent;

    pfree(it);
    return v;
}

/*
 * Worker for "contains" operator's function
 *
 * Formally speaking, containment is top-down, unordered subtree isomorphism.
 *
 * Takes iterators that belong to some container type.  These iterators
 * "belong" to those values in the sense that they've just been initialized in
 * respect of them by the caller (perhaps in a nested fashion).
 *
 * "val" is lhs dynamic, and m_contained is rhs dynamic when called from top
 * level. We determine if m_contained is contained within val.
 */
bool dynamic_deep_contains(dynamic_iterator **val, dynamic_iterator **m_contained)
{
    dynamic_value vval;
    dynamic_value vcontained;
    dynamic_iterator_token rval;
    dynamic_iterator_token rcont;

    /*
     * Guard against queue overflow due to overly complex dynamic.
     *
     * Functions called here independently take this precaution, but that
     * might not be sufficient since this is also a recursive function.
     */
    check_stack_depth();

    rval = dynamic_iterator_next(val, &vval, false);
    rcont = dynamic_iterator_next(m_contained, &vcontained, false);

    if (rval != rcont)
    {
        /*
         * The differing return values can immediately be taken as indicating
         * two differing container types at this nesting level, which is
         * sufficient reason to give up entirely (but it should be the case
         * that they're both some container type).
         */
        Assert(rval == WGT_BEGIN_OBJECT || rval == WGT_BEGIN_ARRAY);
        Assert(rcont == WGT_BEGIN_OBJECT || rcont == WGT_BEGIN_ARRAY);
        return false;
    }
    else if (rcont == WGT_BEGIN_OBJECT)
    {
        Assert(vval.type == DYNAMIC_OBJECT);
        Assert(vcontained.type == DYNAMIC_OBJECT);

        /*
         * If the lhs has fewer pairs than the rhs, it can't possibly contain
         * the rhs.  (This conclusion is safe only because we de-duplicate
         * keys in all dynamic objects; thus there can be no corresponding
         * optimization in the array case.)  The case probably won't arise
         * often, but since it's such a cheap check we may as well make it.
         */
        if (vval.val.object.num_pairs < vcontained.val.object.num_pairs)
            return false;

        // Work through rhs "is it contained within?" object 
        for (;;)
        {
            dynamic_value *lhs_val; // lhs_val is from pair in lhs object 

            rcont = dynamic_iterator_next(m_contained, &vcontained, false);

            /*
             * When we get through caller's rhs "is it contained within?"
             * object without failing to find one of its values, it's
             * contained.
             */
            if (rcont == WGT_END_OBJECT)
                return true;

            Assert(rcont == WGT_KEY);

            // First, find value by key... 
            lhs_val = find_dynamic_value_from_container(
                (*val)->container, GT_FOBJECT, &vcontained);

            if (!lhs_val)
                return false;

            /*
             * ...at this stage it is apparent that there is at least a key
             * match for this rhs pair.
             */
            rcont = dynamic_iterator_next(m_contained, &vcontained, true);

            Assert(rcont == WGT_VALUE);

            /*
             * Compare rhs pair's value with lhs pair's value just found using
             * key
             */
            if (lhs_val->type != vcontained.type)
            {
                return false;
            }
            else if (IS_A_DYNAMIC_SCALAR(lhs_val))
            {
                if (!equals_dynamic_scalar_value(lhs_val, &vcontained))
                    return false;
            }
            else
            {
                // Nested container value (object or array) 
                dynamic_iterator *nestval;
                dynamic_iterator *nest_contained;

                Assert(lhs_val->type == DYNAMIC_BINARY);
                Assert(vcontained.type == DYNAMIC_BINARY);

                nestval = dynamic_iterator_init(lhs_val->val.binary.data);
                nest_contained =
                    dynamic_iterator_init(vcontained.val.binary.data);

                /*
                 * Match "value" side of rhs datum object's pair recursively.
                 * It's a nested structure.
                 *
                 * Note that nesting still has to "match up" at the right
                 * nesting sub-levels.  However, there need only be zero or
                 * more matching pairs (or elements) at each nesting level
                 * (provided the *rhs* pairs/elements *all* match on each
                 * level), which enables searching nested structures for a
                 * single String or other primitive type sub-datum quite
                 * effectively (provided the user constructed the rhs nested
                 * structure such that we "know where to look").
                 *
                 * In other words, the mapping of container nodes in the rhs
                 * "vcontained" dynamic to internal nodes on the lhs is
                 * injective, and parent-child edges on the rhs must be mapped
                 * to parent-child edges on the lhs to satisfy the condition
                 * of containment (plus of course the mapped nodes must be
                 * equal).
                 */
                if (!dynamic_deep_contains(&nestval, &nest_contained))
                    return false;
            }
        }
    }
    else if (rcont == WGT_BEGIN_ARRAY)
    {
        dynamic_value *lhs_conts = NULL;
        uint32 num_lhs_elems = vval.val.array.num_elems;

        Assert(vval.type == DYNAMIC_ARRAY);
        Assert(vcontained.type == DYNAMIC_ARRAY);

        /*
         * Handle distinction between "raw scalar" pseudo arrays, and real
         * arrays.
         *
         * A raw scalar may contain another raw scalar, and an array may
         * contain a raw scalar, but a raw scalar may not contain an array. We
         * don't do something like this for the object case, since objects can
         * only contain pairs, never raw scalars (a pair is represented by an
         * rhs object argument with a single contained pair).
         */
        if (vval.val.array.raw_scalar && !vcontained.val.array.raw_scalar)
            return false;

        // Work through rhs "is it contained within?" array 
        for (;;)
        {
            rcont = dynamic_iterator_next(m_contained, &vcontained, true);

            /*
             * When we get through caller's rhs "is it contained within?"
             * array without failing to find one of its values, it's
             * contained.
             */
            if (rcont == WGT_END_ARRAY)
                return true;

            Assert(rcont == WGT_ELEM);

            if (IS_A_DYNAMIC_SCALAR(&vcontained))
            {
                if (!find_dynamic_value_from_container((*val)->container,
                                                      GT_FARRAY, &vcontained))
                    return false;
            }
            else
            {
                uint32 i;

                /*
                 * If this is first container found in rhs array (at this
                 * depth), initialize temp lhs array of containers
                 */
                if (lhs_conts == NULL)
                {
                    uint32 j = 0;

                    // Make room for all possible values 
                    lhs_conts = palloc(sizeof(dynamic_value) * num_lhs_elems);

                    for (i = 0; i < num_lhs_elems; i++)
                    {
                        // Store all lhs elements in temp array 
                        rcont = dynamic_iterator_next(val, &vval, true);
                        Assert(rcont == WGT_ELEM);

                        if (vval.type == DYNAMIC_BINARY)
                            lhs_conts[j++] = vval;
                    }

                    // No container elements in temp array, so give up now 
                    if (j == 0)
                        return false;

                    // We may have only partially filled array 
                    num_lhs_elems = j;
                }

                // XXX: Nested array containment is O(N^2) 
                for (i = 0; i < num_lhs_elems; i++)
                {
                    // Nested container value (object or array) 
                    dynamic_iterator *nestval;
                    dynamic_iterator *nest_contained;
                    bool contains;

                    nestval =
                        dynamic_iterator_init(lhs_conts[i].val.binary.data);
                    nest_contained =
                        dynamic_iterator_init(vcontained.val.binary.data);

                    contains = dynamic_deep_contains(&nestval, &nest_contained);

                    if (nestval)
                        pfree(nestval);
                    if (nest_contained)
                        pfree(nest_contained);
                    if (contains)
                        break;
                }

                /*
                 * Report rhs container value is not contained if couldn't
                 * match rhs container to *some* lhs cont
                 */
                if (i == num_lhs_elems)
                    return false;
            }
        }
    }
    else
    {
        ereport(ERROR, (errmsg("invalid dynamic container type")));
    }

    ereport(ERROR, (errmsg("unexpectedly fell off end of dynamic container")));
    return false;
}

/*
 * Hash an dynamic_value scalar value, mixing the hash value into an existing
 * hash provided by the caller.
 *
 * Some callers may wish to independently XOR in GT_FOBJECT and GT_FARRAY
 * flags.
 */
void dynamic_hash_scalar_value(const dynamic_value *scalar_val, uint32 *hash)
{
    uint32 tmp;

    // Compute hash value for scalar_val 
    switch (scalar_val->type)
    {
    case DYNAMIC_NULL:
        tmp = 0x01;
        break;
    case DYNAMIC_STRING:
        tmp = DatumGetUInt32(
            hash_any((const unsigned char *)scalar_val->val.string.val,
                     scalar_val->val.string.len));
        break;
    case DYNAMIC_NUMERIC:
        // Must hash equal numerics to equal hash codes 
        tmp = DatumGetUInt32(DirectFunctionCall1(
            hash_numeric, NumericGetDatum(scalar_val->val.numeric)));
        break;
    case DYNAMIC_BOOL:
        tmp = scalar_val->val.boolean ? 0x02 : 0x04;
        break;
    case DYNAMIC_INTEGER:
        tmp = DatumGetUInt32(DirectFunctionCall1(
            hashint8, Int64GetDatum(scalar_val->val.int_value)));
        break;
    case DYNAMIC_FLOAT:
        tmp = DatumGetUInt32(DirectFunctionCall1(
            hashfloat8, Float8GetDatum(scalar_val->val.float_value)));
        break;
    default:
        ereport(ERROR, (errmsg("invalid dynamic scalar type %d to compute hash",
                               scalar_val->type)));
        tmp = 0; // keep compiler quiet 
        break;
    }

    /*
     * Combine hash values of successive keys, values and elements by rotating
     * the previous value left 1 bit, then XOR'ing in the new
     * key/value/element's hash value.
     */
    *hash = (*hash << 1) | (*hash >> 31);
    *hash ^= tmp;
}

/*
 * Hash a value to a 64-bit value, with a seed. Otherwise, similar to
 * dynamic_hash_scalar_value.
 */
void dynamic_hash_scalar_value_extended(const dynamic_value *scalar_val, uint64 *hash, uint64 seed)
{
    uint64 tmp = 0;

    switch (scalar_val->type)
    {
    case DYNAMIC_NULL:
        tmp = seed + 0x01;
        break;
    case DYNAMIC_STRING:
        tmp = DatumGetUInt64(hash_any_extended(
            (const unsigned char *)scalar_val->val.string.val,
            scalar_val->val.string.len, seed));
        break;
    case DYNAMIC_NUMERIC:
        tmp = DatumGetUInt64(DirectFunctionCall2(
            hash_numeric_extended, NumericGetDatum(scalar_val->val.numeric),
            UInt64GetDatum(seed)));
        break;
    case DYNAMIC_BOOL:
        if (seed)
        {
            tmp = DatumGetUInt64(DirectFunctionCall2(
                hashcharextended, BoolGetDatum(scalar_val->val.boolean),
                UInt64GetDatum(seed)));
        }
        else
        {
            tmp = scalar_val->val.boolean ? 0x02 : 0x04;
        }
        break;
    case DYNAMIC_INTEGER:
        tmp = DatumGetUInt64(DirectFunctionCall2(
            hashint8extended, Int64GetDatum(scalar_val->val.int_value),
            UInt64GetDatum(seed)));
        break;
    case DYNAMIC_FLOAT:
        tmp = DatumGetUInt64(DirectFunctionCall2(
            hashfloat8extended, Float8GetDatum(scalar_val->val.float_value),
            UInt64GetDatum(seed)));
        break;
    default:
        ereport(
            ERROR,
            (errmsg("invalid dynamic scalar type %d to compute hash extended",
                    scalar_val->type)));
        break;
    }

    *hash = ROTATE_HIGH_AND_LOW_32BITS(*hash);
    *hash ^= tmp;
}

/*
 * Function to compare two floats, obviously. However, there are a few
 * special cases that we need to cover with regards to NaN and +/-Infinity.
 * NaN is not equal to any other number, including itself. However, for
 * ordering, we need to allow NaN = NaN and NaN > any number including
 * positive infinity -
 *
 *     -Infinity < any number < +Infinity < NaN
 *
 * Note: This is copied from float8_cmp_internal.
 * Note: Special float values can cause exceptions, hence the order of the
 *       comparisons.
 */
static int compare_two_floats_orderability(float8 lhs, float8 rhs)
{
    /*
     * We consider all NANs to be equal and larger than any non-NAN. This is
     * somewhat arbitrary; the important thing is to have a consistent sort
     * order.
     */
    if (isnan(lhs))
    {
        if (isnan(rhs))
            return 0; // NAN = NAN 
        else
            return 1; // NAN > non-NAN 
    }
    else if (isnan(rhs))
    {
        return -1; // non-NAN < NAN 
    }
    else
    {
        if (lhs > rhs)
            return 1;
        else if (lhs < rhs)
            return -1;
        else
            return 0;
    }
}

/*
 * Are two scalar dynamic_values of the same type a and b equal?
 */
static bool equals_dynamic_scalar_value(const dynamic_value *a, const dynamic_value *b)
{
    // if the values are of the same type 
    if (a->type == b->type)
    {
        switch (a->type)
        {
        case DYNAMIC_NULL:
            return true;
        case DYNAMIC_STRING:
            return length_compare_dynamic_string_value(a, b) == 0;
        case DYNAMIC_NUMERIC:
            return DatumGetBool(DirectFunctionCall2(
                numeric_eq, PointerGetDatum(a->val.numeric),
                PointerGetDatum(b->val.numeric)));
        case DYNAMIC_BOOL:
            return a->val.boolean == b->val.boolean;
        case DYNAMIC_INTEGER:
        case DYNAMIC_TIMESTAMP:
        case DYNAMIC_TIME:
            return a->val.int_value == b->val.int_value;
        case DYNAMIC_TIMESTAMPTZ:
            return timestamptz_cmp_internal(a->val.int_value, b->val.int_value) == 0;
        case DYNAMIC_DATE:
            return a->val.date == b->val.date;
        case DYNAMIC_TIMETZ:
            return timetz_cmp_internal(&a->val.timetz, &b->val.timetz) == 0;
        case DYNAMIC_INTERVAL:
            return interval_cmp_internal(&a->val.interval, &b->val.interval) == 0;
        case DYNAMIC_RANGE_INT:
        case DYNAMIC_RANGE_NUM:
        case DYNAMIC_RANGE_DATE:
	case DYNAMIC_RANGE_TS:
        case DYNAMIC_RANGE_TSTZ:
	    return compare_range_internal(a, b) == 0;
	case DYNAMIC_TSQUERY:
	    return CompareTSQ(a->val.tsquery, b->val.tsquery) == 0;
        case DYNAMIC_TSVECTOR:
	    return silly_cmp_tsvector(a->val.tsvector, b->val.tsvector) == 0;
	case DYNAMIC_FLOAT:
            return a->val.float_value == b->val.float_value;
        default:
            ereport(ERROR, (errmsg("invalid dynamic scalar type %d for equals", a->type)));
        }
    }
    // otherwise, the values are of differing type 
    else
        ereport(ERROR, (errmsg("dynamic input scalars must be of same type")));

    // execution will never reach this point due to the ereport call 
    return false;
}

/*
 * Order: haspos, len, word, for all positions (pos, weight)
 */
static int
silly_cmp_tsvector(const TSVector a, const TSVector b)
{
    if (VARSIZE(a) < VARSIZE(b))
         return -1;
    else if (VARSIZE(a) > VARSIZE(b))
         return 1;
    else if (a->size < b->size)
         return -1;
    else if (a->size > b->size)
         return 1;
    else {
         WordEntry *aptr = ARRPTR(a);
         WordEntry *bptr = ARRPTR(b);
         int i = 0;
         int res;

         for (i = 0; i < a->size; i++) {
              if (aptr->haspos != bptr->haspos) {
                   return (aptr->haspos > bptr->haspos) ? -1 : 1;
              } else if ((res = tsCompareString(STRPTR(a) + aptr->pos, aptr->len, STRPTR(b) + bptr->pos, bptr->len, false)) != 0) {
                   return res;
              } else if (aptr->haspos) {
                   WordEntryPos *ap = POSDATAPTR(a, aptr);
                   WordEntryPos *bp = POSDATAPTR(b, bptr);
                   int                     j;

                   if (POSDATALEN(a, aptr) != POSDATALEN(b, bptr))
                        return (POSDATALEN(a, aptr) > POSDATALEN(b, bptr)) ? -1 : 1;

                   for (j = 0; j < POSDATALEN(a, aptr); j++) {
                        if (WEP_GETPOS(*ap) != WEP_GETPOS(*bp))
                             return (WEP_GETPOS(*ap) > WEP_GETPOS(*bp)) ? -1 : 1;
                        else if (WEP_GETWEIGHT(*ap) != WEP_GETWEIGHT(*bp))
                             return (WEP_GETWEIGHT(*ap) > WEP_GETWEIGHT(*bp)) ? -1 : 1;
                        ap++, bp++;
                   }
              }

              aptr++;
              bptr++;
         }
    }

    return 0;
}


static int
CompareTSQ(TSQuery a, TSQuery b) {
    if (a->size != b->size) {
        return (a->size < b->size) ? -1 : 1;
    } else if (VARSIZE(a) != VARSIZE(b)) {
        return (VARSIZE(a) < VARSIZE(b)) ? -1 : 1;
    } else if (a->size != 0) {
        QTNode *an = QT2QTN(GETQUERY(a), GETOPERAND(a));
        QTNode *bn = QT2QTN(GETQUERY(b), GETOPERAND(b));
        int res = QTNodeCompare(an, bn);

        QTNFree(an);
        QTNFree(bn);

        return res;
    }

    return 0;
}

static int compare_range_internal(dynamic_value *a, dynamic_value *b) {
    RangeType *r1 = a->val.range;
    RangeType *r2 = b->val.range;
    TypeCacheEntry *typcache = lookup_type_cache(RangeTypeGetOid(r1), TYPECACHE_RANGE_INFO);
    RangeBound lower1, lower2;
    RangeBound upper1, upper2;
    bool empty1, empty2;

    /* Different types should be prevented by ANYRANGE matching rules */
    if (a->type != b->type)
        elog(ERROR, "range types do not match");

    range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
    range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

    if (empty1 && empty2) 
         return 0;
    if (empty1)
         return 1;
    if (empty2)
         return -1;

    int cmp = range_cmp_bounds(typcache, &lower1, &lower2);
    if (cmp != 0)
        return cmp;

    return range_cmp_bounds(typcache, &upper1, &upper2);
}


/*
 * Compare two scalar dynamic_values, returning -1, 0, or 1.
 *
 * Strings are compared using the default collation.  Used by B-tree
 * operators, where a lexical sort order is generally expected.
 */
int compare_dynamic_scalar_values(dynamic_value *a, dynamic_value *b)
{
    if (a->type == b->type)
    {
        switch (a->type)
        {
        case DYNAMIC_NULL:
            return 0;
        case DYNAMIC_STRING:
            return varstr_cmp(a->val.string.val, a->val.string.len, b->val.string.val, b->val.string.len,
                              DEFAULT_COLLATION_OID);
        case DYNAMIC_NUMERIC:
            return DatumGetInt32(DirectFunctionCall2(
                numeric_cmp, PointerGetDatum(a->val.numeric),
                PointerGetDatum(b->val.numeric)));
        case DYNAMIC_BOOL:
            if (a->val.boolean == b->val.boolean)
                return 0;
            else if (a->val.boolean > b->val.boolean)
                return 1;
            else
                return -1;
        case DYNAMIC_TIMESTAMP:
	    return timestamp_cmp_internal(a->val.int_value, b->val.int_value);
	case DYNAMIC_TIMESTAMPTZ:
	    return timestamptz_cmp_internal(a->val.int_value, b->val.int_value);
	case DYNAMIC_INTEGER:
	case DYNAMIC_TIME:
            if (a->val.int_value == b->val.int_value)
                return 0;
            else if (a->val.int_value > b->val.int_value)
                return 1;
            else
                return -1;
	case DYNAMIC_DATE:
            if (a->val.date == b->val.date)
                return 0;
            else if (a->val.date > b->val.date)
                return 1;
            else
                return -1;
	case DYNAMIC_TIMETZ:
	    return timetz_cmp_internal(&a->val.timetz, &b->val.timetz);
	case DYNAMIC_INTERVAL:
	    return interval_cmp_internal(&a->val.interval, &b->val.interval);
        case DYNAMIC_RANGE_INT:
        case DYNAMIC_RANGE_NUM:
        case DYNAMIC_RANGE_DATE:
        case DYNAMIC_RANGE_TS:
        case DYNAMIC_RANGE_TSTZ:	    
	    return compare_range_internal(a, b);
        case DYNAMIC_TSQUERY:
            return CompareTSQ(a->val.tsquery, b->val.tsquery);
        case DYNAMIC_TSVECTOR:
            return silly_cmp_tsvector(a->val.tsvector, b->val.tsvector);
	case DYNAMIC_FLOAT:
            return compare_two_floats_orderability(a->val.float_value, b->val.float_value);
	default:
            ereport(ERROR, (errmsg("invalid dynamic scalar type %d for compare", a->type)));
        }
    }

    // timestamp and timestamp with timezone
    if (a->type == DYNAMIC_TIMESTAMP && b->type == DYNAMIC_TIMESTAMPTZ)
        return timestamp_cmp_timestamptz_internal(a->val.int_value, b->val.int_value);

    if (a->type == DYNAMIC_TIMESTAMPTZ && b->type == DYNAMIC_TIMESTAMP)
        return -1 * timestamp_cmp_timestamptz_internal(b->val.int_value, a->val.int_value);

    // date and timestamp
    if (a->type == DYNAMIC_DATE && b->type == DYNAMIC_TIMESTAMP)
        return date_cmp_timestamp_internal(a->val.date, b->val.int_value);

    if (a->type == DYNAMIC_TIMESTAMP && b->type == DYNAMIC_DATE)
        return -1 * date_cmp_timestamp_internal(b->val.date, a->val.int_value);

    // date and timestamp with timezone
    if (a->type == DYNAMIC_DATE && b->type == DYNAMIC_TIMESTAMPTZ)
        return date_cmp_timestamptz_internal(a->val.date, b->val.int_value);

    if (a->type == DYNAMIC_TIMESTAMPTZ && b->type == DYNAMIC_DATE)
        return -1 * date_cmp_timestamptz_internal(b->val.date, a->val.int_value);

    // time and time with timezone
    if (a->type == DYNAMIC_TIME && b->type == DYNAMIC_TIMETZ) {
         int64 b_time = DatumGetTimeADT(DirectFunctionCall1(timetz_time, TimeTzADTPGetDatum(&b->val.timetz)));

         if (a->val.int_value == b_time)
            return 0;
         else if (a->val.int_value > b_time)
            return 1;
         else
            return -1;
    }

    if (a->type == DYNAMIC_TIMETZ && b->type == DYNAMIC_TIME) {
         int64 a_time = DatumGetTimeADT(DirectFunctionCall1(timetz_time, TimeTzADTPGetDatum(&a->val.timetz)));

         if (a_time == b->val.int_value)
            return 0;
         else if (a_time > b->val.int_value)
            return 1;
         else
            return -1; 
    }


    // check for integer compared to float 
    if (a->type == DYNAMIC_INTEGER && b->type == DYNAMIC_FLOAT)
        return compare_two_floats_orderability((float8)a->val.int_value, b->val.float_value);
    // check for float compared to integer 
    if (a->type == DYNAMIC_FLOAT && b->type == DYNAMIC_INTEGER)
        return compare_two_floats_orderability(a->val.float_value, (float8)b->val.int_value);
    // check for integer or float compared to numeric 
  /*  if (is_numeric_result(a, b))
    {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_dynamic_value(a);
        rhsd = get_numeric_datum_from_dynamic_value(b);
        numd = DirectFunctionCall2(numeric_cmp, lhsd, rhsd);

        return DatumGetInt32(numd);
    }*/

    ereport(ERROR, (errmsg("dynamic input scalar type mismatch")));
    return -1;
}

/*
 * Functions for manipulating the resizeable buffer used by convert_dynamic and
 * its subroutines.
 */

/*
 * Reserve 'len' bytes, at the end of the buffer, enlarging it if necessary.
 * Returns the offset to the reserved area. The caller is expected to fill
 * the reserved area later with copy_to_buffer().
 */
int reserve_from_buffer(StringInfo buffer, int len)
{
    int offset;

    // Make more room if needed 
    enlargeStringInfo(buffer, len);

    // remember current offset 
    offset = buffer->len;

    // reserve the space 
    buffer->len += len;

    /*
     * Keep a trailing null in place, even though it's not useful for us; it
     * seems best to preserve the invariants of StringInfos.
     */
    buffer->data[buffer->len] = '\0';

    return offset;
}

/*
 * Copy 'len' bytes to a previously reserved area in buffer.
 */
static void copy_to_buffer(StringInfo buffer, int offset, const char *data, int len)
{
    memcpy(buffer->data + offset, data, len);
}

/*
 * A shorthand for reserve_from_buffer + copy_to_buffer.
 */
static void append_to_buffer(StringInfo buffer, const char *data, int len)
{
    int offset;

    offset = reserve_from_buffer(buffer, len);
    copy_to_buffer(buffer, offset, data, len);
}

/*
 * Append padding, so that the length of the StringInfo is int-aligned.
 * Returns the number of padding bytes appended.
 */
short pad_buffer_to_int(StringInfo buffer)
{
    int padlen;
    int p;
    int offset;

    padlen = INTALIGN(buffer->len) - buffer->len;

    offset = reserve_from_buffer(buffer, padlen);

    // padlen must be small, so this is probably faster than a memset 
    for (p = 0; p < padlen; p++)
        buffer->data[offset + p] = '\0';

    return padlen;
}

/*
 * Given an dynamic_value, convert to dynamic. The result is palloc'd.
 */
static dynamic *convert_to_dynamic(dynamic_value *val)
{
    StringInfoData buffer;
    gtentry aentry;
    dynamic *res;

    // Should not already have binary representation 
    Assert(val->type != DYNAMIC_BINARY);

    // Allocate an output buffer. It will be enlarged as needed 
    initStringInfo(&buffer);

    // Make room for the varlena header 
    reserve_from_buffer(&buffer, VARHDRSZ);

    convert_dynamic_value(&buffer, &aentry, val, 0);

    /*
     * Note: the gtentry of the root is discarded. Therefore the root
     * dynamic_container struct must contain enough information to tell what
     * kind of value it is.
     */

    res = (dynamic *)buffer.data;

    SET_VARSIZE(res, buffer.len);

    return res;
}


/*
 * Subroutine of convert_dynamic: serialize a single dynamic_value into buffer.
 *
 * The gtentry header for this node is returned in *header.  It is filled in
 * with the length of this value and appropriate type bits.  If we wish to
 * store an end offset rather than a length, it is the caller's responsibility
 * to adjust for that.
 *
 * If the value is an array or an object, this recurses. 'level' is only used
 * for debugging purposes.
 */
static void convert_dynamic_value(StringInfo buffer, gtentry *header,
                                 dynamic_value *val, int level)
{
    check_stack_depth();

    if (!val)
        return;

    /*
     * An dynamic_value passed as val should never have a type of DYNAMIC_BINARY,
     * and neither should any of its sub-components. Those values will be
     * produced by convert_dynamic_array and convert_dynamic_object, the results
     * of which will not be passed back to this function as an argument.
     */

    if (IS_A_DYNAMIC_SCALAR(val))
        convert_dynamic_scalar(buffer, header, val);
    else if (val->type == DYNAMIC_ARRAY)
        convert_dynamic_array(buffer, header, val, level);
    else if (val->type == DYNAMIC_OBJECT)
        convert_dynamic_object(buffer, header, val, level);
    else
        ereport(ERROR,
                (errmsg("unknown dynamic type %d to convert", val->type)));
}

// define the type and size of the agt_header 
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


static void convert_dynamic_array(StringInfo buffer, gtentry *pheader,
                                 dynamic_value *val, int level)
{
    int base_offset;
    int gtentry_offset;
    int i;
    int totallen;
    uint32 header;
    int num_elems = val->val.array.num_elems;

    // Remember where in the buffer this array starts. 
    base_offset = buffer->len;

    // Align to 4-byte boundary (any padding counts as part of my data) 
    pad_buffer_to_int(buffer);

    /*
     * Construct the header gtentry and store it in the beginning of the
     * variable-length payload.
     */
    header = num_elems | GT_FARRAY;
    if (val->val.array.raw_scalar)
    {
        Assert(num_elems == 1);
        Assert(level == 0);
        header |= GT_FSCALAR;
    }

    append_to_buffer(buffer, (char *)&header, sizeof(uint32));

    // Reserve space for the gtentrys of the elements. 
    gtentry_offset = reserve_from_buffer(buffer, sizeof(gtentry) * num_elems);

    totallen = 0;
    for (i = 0; i < num_elems; i++)
    {
        dynamic_value *elem = &val->val.array.elems[i];
        int len;
        gtentry meta;

        /*
         * Convert element, producing a gtentry and appending its
         * variable-length data to buffer
         */
        convert_dynamic_value(buffer, &meta, elem, level + 1);

        len = GTE_OFFLENFLD(meta);
        totallen += len;

        /*
         * Bail out if total variable-length data exceeds what will fit in a
         * gtentry length field.  We check this in each iteration, not just
         * once at the end, to forestall possible integer overflow.
         */
        if (totallen > GTENTRY_OFFLENMASK)
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg(
                     "total size of dynamic array elements exceeds the maximum of %u bytes",
                     GTENTRY_OFFLENMASK)));
        }

        /*
         * Convert each GT_OFFSET_STRIDE'th length to an offset.
         */
        if ((i % GT_OFFSET_STRIDE) == 0)
            meta = (meta & GTENTRY_TYPEMASK) | totallen | GTENTRY_HAS_OFF;

        copy_to_buffer(buffer, gtentry_offset, (char *)&meta,
                       sizeof(gtentry));
        gtentry_offset += sizeof(gtentry);
    }

    // Total data size is everything we've appended to buffer 
    totallen = buffer->len - base_offset;

    // Check length again, since we didn't include the metadata above 
    if (totallen > GTENTRY_OFFLENMASK)
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg(
                 "total size of dynamic array elements exceeds the maximum of %u bytes",
                 GTENTRY_OFFLENMASK)));
    }

    // Initialize the header of this node in the container's gtentry array 
    *pheader = GTENTRY_IS_CONTAINER | totallen;
}

static void convert_dynamic_object(StringInfo buffer, gtentry *pheader,
                                  dynamic_value *val, int level)
{
    int base_offset;
    int gtentry_offset;
    int i;
    int totallen;
    uint32 header;
    int num_pairs = val->val.object.num_pairs;

    // Remember where in the buffer this object starts. 
    base_offset = buffer->len;

    // Align to 4-byte boundary (any padding counts as part of my data) 
    pad_buffer_to_int(buffer);

    /*
     * Construct the header gtentry and store it in the beginning of the
     * variable-length payload.
     */
    header = num_pairs | GT_FOBJECT;
    append_to_buffer(buffer, (char *)&header, sizeof(uint32));

    // Reserve space for the gtentrys of the keys and values. 
    gtentry_offset = reserve_from_buffer(buffer,
                                          sizeof(gtentry) * num_pairs * 2);

    /*
     * Iterate over the keys, then over the values, since that is the ordering
     * we want in the on-disk representation.
     */
    totallen = 0;
    for (i = 0; i < num_pairs; i++)
    {
        dynamic_pair *pair = &val->val.object.pairs[i];
        int len;
        gtentry meta;

        /*
         * Convert key, producing an gtentry and appending its variable-length
         * data to buffer
         */
        convert_dynamic_scalar(buffer, &meta, &pair->key);

        len = GTE_OFFLENFLD(meta);
        totallen += len;

        /*
         * Bail out if total variable-length data exceeds what will fit in a
         * gtentry length field.  We check this in each iteration, not just
         * once at the end, to forestall possible integer overflow.
         */
        if (totallen > GTENTRY_OFFLENMASK)
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg(
                     "total size of dynamic object elements exceeds the maximum of %u bytes",
                     GTENTRY_OFFLENMASK)));
        }

        /*
         * Convert each GT_OFFSET_STRIDE'th length to an offset.
         */
        if ((i % GT_OFFSET_STRIDE) == 0)
            meta = (meta & GTENTRY_TYPEMASK) | totallen | GTENTRY_HAS_OFF;

        copy_to_buffer(buffer, gtentry_offset, (char *)&meta,
                       sizeof(gtentry));
        gtentry_offset += sizeof(gtentry);
    }
    for (i = 0; i < num_pairs; i++)
    {
        dynamic_pair *pair = &val->val.object.pairs[i];
        int len;
        gtentry meta;

        /*
         * Convert value, producing an gtentry and appending its
         * variable-length data to buffer
         */
        convert_dynamic_value(buffer, &meta, &pair->value, level + 1);

        len = GTE_OFFLENFLD(meta);
        totallen += len;

        /*
         * Bail out if total variable-length data exceeds what will fit in a
         * gtentry length field.  We check this in each iteration, not just
         * once at the end, to forestall possible integer overflow.
         */
        if (totallen > GTENTRY_OFFLENMASK)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("total size of dynamic object elements exceeds the maximum of %u bytes",
                     GTENTRY_OFFLENMASK)));

        /*
         * Convert each GT_OFFSET_STRIDE'th length to an offset.
         */
        if (((i + num_pairs) % GT_OFFSET_STRIDE) == 0)
            meta = (meta & GTENTRY_TYPEMASK) | totallen | GTENTRY_HAS_OFF;

        copy_to_buffer(buffer, gtentry_offset, (char *)&meta,
                       sizeof(gtentry));
        gtentry_offset += sizeof(gtentry);
    }

    // Total data size is everything we've appended to buffer 
    totallen = buffer->len - base_offset;

    // Check length again, since we didn't include the metadata above 
    if (totallen > GTENTRY_OFFLENMASK)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg("total size of dynamic object elements exceeds the maximum of %u bytes", GTENTRY_OFFLENMASK)));

    // Initialize the header of this node in the container's gtentry array 
    *pheader = GTENTRY_IS_CONTAINER | totallen;
}

static void convert_dynamic_scalar(StringInfo buffer, gtentry *entry, dynamic_value *scalar_val)
{
    int numlen;
    short padlen;
    bool status;

    switch (scalar_val->type)
    {
    case DYNAMIC_NULL:
        *entry = GTENTRY_IS_NULL;
        break;

    case DYNAMIC_STRING:
        append_to_buffer(buffer, scalar_val->val.string.val, scalar_val->val.string.len);

        *entry = scalar_val->val.string.len;
        break;

    case DYNAMIC_NUMERIC:
        numlen = VARSIZE_ANY(scalar_val->val.numeric);
        padlen = pad_buffer_to_int(buffer);

        append_to_buffer(buffer, (char *)scalar_val->val.numeric, numlen);

        *entry = GTENTRY_IS_NUMERIC | (padlen + numlen);
        break;

    case DYNAMIC_BOOL:
        *entry = (scalar_val->val.boolean) ? GTENTRY_IS_BOOL_TRUE : GTENTRY_IS_BOOL_FALSE;
        break;

    default:
        // returns true if there was a valid extended type processed 
        status = ag_serialize_extended_type(buffer, entry, scalar_val);
        // if nothing was found, error log out 
        if (!status)
            ereport(ERROR, (errmsg("invalid dynamic scalar type %d to convert",
                                   scalar_val->type)));
    }
}

/*
 * Compare two DYNAMIC_STRING dynamic_value values, a and b.
 *
 * This is a special qsort() comparator used to sort strings in certain
 * internal contexts where it is sufficient to have a well-defined sort order.
 * In particular, object pair keys are sorted according to this criteria to
 * facilitate cheap binary searches where we don't care about lexical sort
 * order.
 *
 * a and b are first sorted based on their length.  If a tie-breaker is
 * required, only then do we consider string binary equality.
 */
static int length_compare_dynamic_string_value(const void *a, const void *b)
{
    const dynamic_value *va = (const dynamic_value *)a;
    const dynamic_value *vb = (const dynamic_value *)b;
    int res;

    Assert(va->type == DYNAMIC_STRING);
    Assert(vb->type == DYNAMIC_STRING);

    if (va->val.string.len == vb->val.string.len)
    {
        res = memcmp(va->val.string.val, vb->val.string.val,
                     va->val.string.len);
    }
    else
    {
        res = (va->val.string.len > vb->val.string.len) ? 1 : -1;
    }

    return res;
}

/*
 * qsort_arg() comparator to compare dynamic_pair values.
 *
 * Third argument 'binequal' may point to a bool. If it's set, *binequal is set
 * to true iff a and b have full binary equality, since some callers have an
 * interest in whether the two values are equal or merely equivalent.
 *
 * N.B: String comparisons here are "length-wise"
 *
 * Pairs with equals keys are ordered such that the order field is respected.
 */
static int length_compare_dynamic_pair(const void *a, const void *b,
                                      void *binequal)
{
    const dynamic_pair *pa = (const dynamic_pair *)a;
    const dynamic_pair *pb = (const dynamic_pair *)b;
    int res;

    res = length_compare_dynamic_string_value(&pa->key, &pb->key);
    if (res == 0 && binequal)
        *((bool *)binequal) = true;

    /*
     * Guarantee keeping order of equal pair.  Unique algorithm will prefer
     * first element as value.
     */
    if (res == 0)
        res = (pa->order > pb->order) ? -1 : 1;

    return res;
}

/*
 * Sort and unique-ify pairs in dynamic_value object
 */
void uniqueify_dynamic_object(dynamic_value *object)
{
    bool has_non_uniq = false;

    Assert(object->type == DYNAMIC_OBJECT);

    if (object->val.object.num_pairs > 1)
        qsort_arg(object->val.object.pairs, object->val.object.num_pairs,
                  sizeof(dynamic_pair), length_compare_dynamic_pair,
                  &has_non_uniq);

    if (has_non_uniq)
    {
        dynamic_pair *ptr = object->val.object.pairs + 1;
        dynamic_pair *res = object->val.object.pairs;

        while (ptr - object->val.object.pairs < object->val.object.num_pairs)
        {
            // Avoid copying over duplicate 
            if (length_compare_dynamic_string_value(ptr, res) != 0)
            {
                res++;
                if (ptr != res)
                    memcpy(res, ptr, sizeof(dynamic_pair));
            }
            ptr++;
        }

        object->val.object.num_pairs = res + 1 - object->val.object.pairs;
    }
}

char *dynamic_value_type_to_string(enum dynamic_value_type type)
{
    switch (type)
    {
        case DYNAMIC_NULL:
            return "NULL";
        case DYNAMIC_STRING:
            return "string";
        case DYNAMIC_NUMERIC:
            return "numeric";
        case DYNAMIC_INTEGER:
            return "integer";
        case DYNAMIC_FLOAT:
            return "float";
        case DYNAMIC_BOOL:
            return "boolean";
        case DYNAMIC_ARRAY:
            return "array";
        case DYNAMIC_OBJECT:
            return "map";
        case DYNAMIC_BINARY:
            return "binary";
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("unknown dynamic")));
    }

    return NULL;
}
