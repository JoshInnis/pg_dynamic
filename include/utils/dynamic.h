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

#ifndef AG_DYNAMIC_H
#define AG_DYNAMIC_H

#include "access/htup_details.h"
#include "c.h"
#include "datatype/timestamp.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "tsearch/ts_type.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/inet.h"
#include "utils/geo_decls.h"
#include "utils/multirangetypes.h"
#include "utils/numeric.h"
#include "utils/rangetypes.h"
#include "utils/syscache.h"


#include "catalog/pg_type.h"


/* Tokens used when sequentially processing an dynamic value */
typedef enum
{
    WGT_DONE,
    WGT_KEY,
    WGT_VALUE,
    WGT_VECTOR_VALUE,
    WGT_ELEM,
    WGT_BEGIN_ARRAY,
    WGT_END_ARRAY,
    WGT_BEGIN_OBJECT,
    WGT_END_OBJECT,
    WGT_BEGIN_VECTOR,
    WGT_END_VECTOR
} dynamic_iterator_token;

#define DYNAMIC_ITERATOR_TOKEN_IS_HASHABLE(x) \
    (x > WGT_DONE && x < WGT_BEGIN_ARRAY)

/* Strategy numbers for GIN index opclasses */
#define DYNAMIC_CONTAINS_STRATEGY_NUMBER    7
#define DYNAMIC_EXISTS_STRATEGY_NUMBER      9
#define DYNAMIC_EXISTS_ANY_STRATEGY_NUMBER 10
#define DYNAMIC_EXISTS_ALL_STRATEGY_NUMBER 11

/*
 * In the standard dynamic_ops GIN opclass for dynamic, we choose to index both
 * keys and values.  The storage format is text.  The first byte of the text
 * string distinguishes whether this is a key (always a string), null value,
 * boolean value, numeric value, or string value.  However, array elements
 * that are strings are marked as though they were keys; this imprecision
 * supports the definition of the "exists" operator, which treats array
 * elements like keys.  The remainder of the text string is empty for a null
 * value, "t" or "f" for a boolean value, a normalized print representation of
 * a numeric value, or the text of a string value.  However, if the length of
 * this text representation would exceed GT_GIN_MAX_LENGTH bytes, we instead
 * hash the text representation and store an 8-hex-digit representation of the
 * uint32 hash value, marking the prefix byte with an additional bit to
 * distinguish that this has happened.  Hashing long strings saves space and
 * ensures that we won't overrun the maximum entry length for a GIN index.
 * (But GT_GIN_MAX_LENGTH is quite a bit shorter than GIN's limit.  It's
 * chosen to ensure that the on-disk text datum will have a short varlena
 * header.) Note that when any hashed item appears in a query, we must recheck
 * index matches against the heap tuple; currently, this costs nothing because
 * we must always recheck for other reasons.
 */
#define GT_GIN_FLAG_KEY    0x01 // key (or string array element)
#define GT_GIN_FLAG_NULL   0x02 // null value
#define GT_GIN_FLAG_BOOL   0x03 // boolean value
#define GT_GIN_FLAG_NUM    0x04 // numeric value
#define GT_GIN_FLAG_STR    0x05 // string value (if not an array element)
#define GT_GIN_FLAG_HASHED 0x10 // OR'd into flag if value was hashed
#define GT_GIN_MAX_LENGTH   125 // max length of text part before hashing

/* Convenience macros */
#define DATUM_GET_DYNAMIC_P(d) ((dynamic *)PG_DETOAST_DATUM(d))
#define DYNAMIC_P_GET_DATUM(p) PointerGetDatum(p)
#define AG_GET_ARG_DYNAMIC_P(x) DATUM_GET_DYNAMIC_P(PG_GETARG_DATUM(x))
#define AG_RETURN_DYNAMIC_P(x) PG_RETURN_POINTER(x)

typedef struct dynamic_pair dynamic_pair;
typedef struct dynamic_value dynamic_value;

/*
 * dynamics are varlena objects, so must meet the varlena convention that the
 * first int32 of the object contains the total object size in bytes.  Be sure
 * to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * dynamic is the on-disk representation, in contrast to the in-memory
 * dynamic_value representation.  Often, dynamic_values are just shims through
 * which a dynamic buffer is accessed, but they can also be deep copied and
 * passed around.
 *
 * dynamic is a tree structure. Each node in the tree consists of an gtentry
 * header and a variable-length content (possibly of zero size).  The gtentry
 * header indicates what kind of a node it is, e.g. a string or an array,
 * and provides the length of its variable-length portion.
 *
 * The gtentry and the content of a node are not stored physically together.
 * Instead, the container array or object has an array that holds the gtentrys
 * of all the child nodes, followed by their variable-length portions.
 *
 * The root node is an exception; it has no parent array or object that could
 * hold its gtentry. Hence, no gtentry header is stored for the root node.
 * It is implicitly known that the root node must be an array or an object,
 * so we can get away without the type indicator as long as we can distinguish
 * the two.  For that purpose, both an array and an object begin with a uint32
 * header field, which contains an GT_FOBJECT or GT_FARRAY flag.  When a
 * naked scalar value needs to be stored as an dynamic value, what we actually
 * store is an array with one element, with the flags in the array's header
 * field set to GT_FSCALAR | GT_FARRAY.
 *
 * Overall, the dynamic struct requires 4-bytes alignment. Within the struct,
 * the variable-length portion of some node types is aligned to a 4-byte
 * boundary, while others are not. When alignment is needed, the padding is
 * in the beginning of the node that requires it. For example, if a numeric
 * node is stored after a string node, so that the numeric node begins at
 * offset 3, the variable-length portion of the numeric node will begin with
 * one padding byte so that the actual numeric data is 4-byte aligned.
 */

/*
 * gtentry format.
 *
 * The least significant 28 bits store either the data length of the entry,
 * or its end+1 offset from the start of the variable-length portion of the
 * containing object.  The next three bits store the type of the entry, and
 * the high-order bit tells whether the least significant bits store a length
 * or an offset.
 *
 * The reason for the offset-or-length complication is to compromise between
 * access speed and data compressibility.  In the initial design each gtentry
 * always stored an offset, but this resulted in gtentry arrays with horrible
 * compressibility properties, so that TOAST compression of an dynamic did not
 * work well.  Storing only lengths would greatly improve compressibility,
 * but it makes random access into large arrays expensive (O(N) not O(1)).
 * So what we do is store an offset in every GT_OFFSET_STRIDE'th gtentry and
 * a length in the rest.  This results in reasonably compressible data (as
 * long as the stride isn't too small).  We may have to examine as many as
 * GT_OFFSET_STRIDE gtentrys in order to find out the offset or length of any
 * given item, but that's still O(1) no matter how large the container is.
 *
 * We could avoid eating a flag bit for this purpose if we were to store
 * the stride in the container header, or if we were willing to treat the
 * stride as an unchangeable constant.  Neither of those options is very
 * attractive though.
 */
typedef uint32 gtentry;

#define GTENTRY_OFFLENMASK 0x0FFFFFFF
#define GTENTRY_TYPEMASK   0x70000000
#define GTENTRY_HAS_OFF    0x80000000

/* values stored in the type bits */
#define GTENTRY_IS_STRING     0x00000000
#define GTENTRY_IS_NUMERIC    0x10000000
#define GTENTRY_IS_BOOL_FALSE 0x20000000
#define GTENTRY_IS_BOOL_TRUE  0x30000000
#define GTENTRY_IS_NULL       0x40000000
#define GTENTRY_IS_CONTAINER  0x50000000 /* array or object */
#define GTENTRY_IS_DYNAMIC     0x60000000 /* extended type designator */

/* Access macros.  Note possible multiple evaluations */
#define GTE_OFFLENFLD(agte_) \
    ((agte_)&GTENTRY_OFFLENMASK)
#define GTE_HAS_OFF(agte_) \
    (((agte_)&GTENTRY_HAS_OFF) != 0)
#define GTE_IS_STRING(agte_) \
    (((agte_)&GTENTRY_TYPEMASK) == GTENTRY_IS_STRING)
#define GTE_IS_NUMERIC(agte_) \
    (((agte_)&GTENTRY_TYPEMASK) == GTENTRY_IS_NUMERIC)
#define GTE_IS_CONTAINER(agte_) \
    (((agte_)&GTENTRY_TYPEMASK) == GTENTRY_IS_CONTAINER)
#define GTE_IS_NULL(agte_) \
    (((agte_)&GTENTRY_TYPEMASK) == GTENTRY_IS_NULL)
#define GTE_IS_BOOL_TRUE(agte_) \
    (((agte_)&GTENTRY_TYPEMASK) == GTENTRY_IS_BOOL_TRUE)
#define GTE_IS_BOOL_FALSE(agte_) \
    (((agte_)&GTENTRY_TYPEMASK) == GTENTRY_IS_BOOL_FALSE)
#define GTE_IS_BOOL(agte_) \
    (GTE_IS_BOOL_TRUE(agte_) || GTE_IS_BOOL_FALSE(agte_))
#define GTE_IS_DYNAMIC(agte_) \
    (((agte_)&GTENTRY_TYPEMASK) == GTENTRY_IS_DYNAMIC)

/* Macro for advancing an offset variable to the next gtentry */
#define GTE_ADVANCE_OFFSET(offset, agte) \
    do \
    { \
        gtentry agte_ = (agte); \
        if (GTE_HAS_OFF(agte_)) \
            (offset) = GTE_OFFLENFLD(agte_); \
        else \
            (offset) += GTE_OFFLENFLD(agte_); \
    } while (0)

/*
 * We store an offset, not a length, every GT_OFFSET_STRIDE children.
 * Caution: this macro should only be referenced when creating an dynamic
 * value.  When examining an existing value, pay attention to the HAS_OFF
 * bits instead.  This allows changes in the offset-placement heuristic
 * without breaking on-disk compatibility.
 */
#define GT_OFFSET_STRIDE 32

/*
 * An dynamic array or object node, within an dynamic Datum.
 *
 * An array has one child for each element, stored in array order.
 *
 * An object has two children for each key/value pair.  The keys all appear
 * first, in key sort order; then the values appear, in an order matching the
 * key order.  This arrangement keeps the keys compact in memory, making a
 * search for a particular key more cache-friendly.
 */
typedef struct dynamic_container
{
    uint32 header; /* number of elements or key/value pairs, and flags */
    gtentry children[FLEXIBLE_ARRAY_MEMBER];

    /* the data for each child node follows. */
} dynamic_container;

/* flags for the header-field in dynamic_container*/
#define GT_CMASK   0x0FFFFFFF /* mask for count field */
#define GT_FSCALAR 0x10000000 /* flag bits */
#define GT_FOBJECT 0x20000000
#define GT_FARRAY  0x40000000
#define GT_FBINARY 0x80000000

/* convenience macros for accessing an dynamic_container struct */
#define DYNAMIC_CONTAINER_SIZE(agtc)       ((agtc)->header & GT_CMASK)
#define DYNAMIC_CONTAINER_IS_SCALAR(agtc) (((agtc)->header & GT_FSCALAR) != 0)
#define DYNAMIC_CONTAINER_IS_OBJECT(agtc) (((agtc)->header & GT_FOBJECT) != 0)
#define DYNAMIC_CONTAINER_IS_ARRAY(agtc)  (((agtc)->header & GT_FARRAY)  != 0)
#define DYNAMIC_CONTAINER_IS_BINARY(agtc) (((agtc)->header & GT_FBINARY) != 0)
#define DYNAMIC_CONTAINER_IS_EXTENDED_COMPOSITE(agtc) (((agtc)->header & GT_FEXTENDED_COMPOSITE) != 0)

// The top-level on-disk format for an dynamic datum.
typedef struct
{
    int32 vl_len_; // varlena header (do not touch directly!)
    dynamic_container root;
} dynamic;

// convenience macros for accessing the root container in an dynamic datum
#define DYNA_ROOT_COUNT(agtp_) (*(uint32 *)VARDATA(agtp_) & GT_CMASK)
#define DYNA_ROOT_IS_SCALAR(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & GT_FSCALAR) != 0)
#define DYNA_ROOT_IS_OBJECT(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & GT_FOBJECT) != 0)
#define DYNA_ROOT_IS_ARRAY(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & GT_FARRAY) != 0)
#define DYNA_ROOT_IS_BINARY(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & GT_FBINARY) != 0)
#define DYNA_ROOT_IS_EXTENDED_COMPOSITE(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & GT_FEXTENDED_COMPOSITE) != 0)
#define DYNA_ROOT_BINARY_FLAGS(agtp_) \
    (*(uint32 *)VARDATA(agtp_) & GT_FBINARY_MASK)

// values for the DYNAMIC header field to denote the stored data type
#define DYNA_HEADER_INTEGER          0x00000000
#define DYNA_HEADER_FLOAT            0x00000001
#define DYNA_HEADER_TIMESTAMP        0x00000002
#define DYNA_HEADER_TIMESTAMPTZ      0x00000003
#define DYNA_HEADER_DATE             0x00000004
#define DYNA_HEADER_TIME             0x00000005
#define DYNA_HEADER_TIMETZ           0x00000006
#define DYNA_HEADER_INTERVAL         0x00000007
#define DYNA_HEADER_INET             0x00000009
#define DYNA_HEADER_CIDR             0x0000000A
#define DYNA_HEADER_MAC              0x0000000B
#define DYNA_HEADER_MAC8             0x0000000C
#define DYNA_HEADER_POINT            0x0000000D
#define DYNA_HEADER_PATH             0x0000000E
#define DYNA_HEADER_LSEG             0x0000000F
#define DYNA_HEADER_LINE             0x00000010
#define DYNA_HEADER_POLYGON          0x00000011
#define DYNA_HEADER_CIRCLE           0x00000012
#define DYNA_HEADER_BOX              0x00000013
#define DYNA_HEADER_BOX2D            0x00000014
#define DYNA_HEADER_BOX3D            0x00000015
#define DYNA_HEADER_SPHEROID         0x00000016
#define DYNA_HEADER_GSERIALIZED      0x00000017
#define DYNA_HEADER_TSVECTOR         0x00000018
#define DYNA_HEADER_TSQUERY          0x00000019
#define DYNA_HEADER_RANGE_INT        0x0000001A
#define DYNA_HEADER_RANGE_NUM        0x0000001B
#define DYNA_HEADER_RANGE_TS         0x0000001C
#define DYNA_HEADER_RANGE_TSTZ       0x0000001D
#define DYNA_HEADER_RANGE_DATE       0x0000001E
#define DYNA_HEADER_RANGE_INT_MULTI  0x0000001F
#define DYNA_HEADER_RANGE_NUM_MULTI  0x00000020
#define DYNA_HEADER_RANGE_TS_MULTI   0x00000021
#define DYNA_HEADER_RANGE_TSTZ_MULTI 0x00000022
#define DYNA_HEADER_RANGE_DATE_MULTI 0x00000023
#define DYNA_HEADER_BYTEA            0x00000024


#define DYNA_IS_INTEGER(agte_) \
    (((agte_) == DYNA_HEADER_INTEGER))

#define DYNA_IS_FLOAT(agte_) \
    (((agte_) == DYNA_HEADER_FLOAT))

#define DYNAMIC_IS_INTEGER(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_INTEGER)

#define DYNAMIC_IS_FLOAT(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_FLOAT)

#define DYNA_IS_TIMESTAMP(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_TIMESTAMP)

#define DYNA_IS_TIMESTAMPTZ(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_TIMESTAMPTZ)

#define DYNA_IS_DATE(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_DATE)

#define DYNA_IS_INTERVAL(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_INTERVAL)

#define DYNA_IS_TIME(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_TIME)

#define DYNA_IS_TIMETZ(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_TIMETZ)

#define DYNA_IS_INET(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_INET)

#define DYNA_IS_CIDR(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_CIDR)

#define DYNA_IS_MACADDR(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_MAC)

#define DYNA_IS_MACADDR8(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_MAC8)

#define DYNA_IS_POINT(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_POINT)

#define DYNA_IS_PATH(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_PATH)

#define DYNA_IS_LSEG(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_LSEG)

#define DYNA_IS_LINE(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_LINE)

#define DYNA_IS_POLYGON(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_POLYGON)

#define DYNA_IS_CIRCLE(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_CIRCLE)

#define DYNA_IS_BOX(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_BOX)

#define DYNA_IS_BOX2D(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_BOX2D)

#define DYNA_IS_BOX3D(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_BOX3D)

#define DYNA_IS_SPHEROID(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_SPHEREOID)

#define DYNA_IS_GSERIALIZED(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_GSERIALIZED)

#define DYNA_IS_GEOMETRY(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_GSERIALIZED)

#define DYNA_IS_TSVECTOR(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_TSVECTOR)

#define DYNA_IS_TSQUERY(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_TSQUERY)

#define DYNA_IS_RANGE_INT(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_RANGE_INT)

#define DYNA_IS_RANGE_NUM(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_RANGE_NUM)

#define DYNA_IS_RANGE_TS(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_RANGE_TS)

#define DYNA_IS_RANGE_TSTZ(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_RANGE_TSTZ)

#define DYNA_IS_RANGE_DATE(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_RANGE_DATE)

#define DYNA_IS_RANGE_INT_MULTI(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_RANGE_INT_MULTI)

#define DYNA_IS_RANGE_TS_MULTI(agt) \
    (GTE_IS_DYNAMIC(agt->root.children[0]) && agt->root.children[1] == DYNA_HEADER_RANGE_TS_MULTI)

enum dynamic_value_type
{
    /* Scalar types */
    DYNAMIC_NULL = 0x0,
    DYNAMIC_STRING,
    DYNAMIC_NUMERIC,
    DYNAMIC_INTEGER,
    DYNAMIC_FLOAT,
    DYNAMIC_BOOL,
    DYNAMIC_TIMESTAMP,
    DYNAMIC_TIMESTAMPTZ,
    DYNAMIC_DATE,
    DYNAMIC_TIME,
    DYNAMIC_TIMETZ,
    DYNAMIC_INTERVAL,
    DYNAMIC_INET,
    DYNAMIC_CIDR,
    DYNAMIC_MAC,
    DYNAMIC_MAC8,
    DYNAMIC_POINT,
    DYNAMIC_LSEG,
    DYNAMIC_LINE,
    DYNAMIC_PATH,
    DYNAMIC_POLYGON,
    DYNAMIC_CIRCLE,
    DYNAMIC_BOX,
    DYNAMIC_BYTEA,
    DYNAMIC_TSVECTOR,
    DYNAMIC_TSQUERY,
    DYNAMIC_RANGE_INT,
    DYNAMIC_RANGE_NUM,
    DYNAMIC_RANGE_TS,
    DYNAMIC_RANGE_TSTZ,
    DYNAMIC_RANGE_DATE,
    DYNAMIC_RANGE_INT_MULTI,
    DYNAMIC_RANGE_NUM_MULTI,
    DYNAMIC_RANGE_TS_MULTI,
    DYNAMIC_RANGE_TSTZ_MULTI,
    DYNAMIC_RANGE_DATE_MULTI,
    /* Composite types */
    DYNAMIC_ARRAY = 0x100,
    DYNAMIC_OBJECT,
    /* Binary (i.e. struct dynamic) DYNAMIC_ARRAY/DYNAMIC_OBJECT */
    DYNAMIC_BINARY
};

/*
 * dynamic_value: In-memory representation of dynamic.  This is a convenient
 * deserialized representation, that can easily support using the "val"
 * union across underlying types during manipulation.  The dynamic on-disk
 * representation has various alignment considerations.
 */
struct dynamic_value
{
    enum dynamic_value_type type;
    union {
        int64 int_value; // 8-byte Integer
        float8 float_value; // 8-byte Float
        Numeric numeric;
        bool boolean;
	    Interval interval;
	    DateADT date;
	    TimeTzADT timetz;
        struct { int len; char *val; /* Not necessarily null-terminated */ } string; // String primitive type
        struct { int num_elems; dynamic_value *elems; bool raw_scalar; } array;       // Array container type
	    struct { int num_pairs; dynamic_pair *pairs; } object;                        // Associative container type
        bytea *bytea;
	    inet inet;
	    macaddr mac;
        macaddr8 mac8;
        Point *point;
        LSEG *lseg;
        LINE *line;
        PATH *path;
        POLYGON *polygon;
        CIRCLE *circle;
        BOX *box;
	    TSVector tsvector;
	    TSQuery tsquery;
	    RangeType *range;
	    MultirangeType *multirange;
        void *extended;
	struct { int len; dynamic_container *data; } binary; // Array or object, in on-disk format
    } val;
};

#define IS_A_DYNAMIC_SCALAR(dynamic_val) \
    ((dynamic_val)->type >= DYNAMIC_NULL && (dynamic_val)->type < DYNAMIC_ARRAY)

/*
 * Key/value pair within an Object.
 *
 * Pairs with duplicate keys are de-duplicated.  We store the originally
 * observed pair ordering for the purpose of removing duplicates in a
 * well-defined way (which is "last observed wins").
 */
struct dynamic_pair
{
    dynamic_value key; /* Must be a DYNAMIC_STRING */
    dynamic_value value; /* May be of any type */
    uint32 order; /* Pair's index in original sequence */
};

/* Conversion state used when parsing dynamic from text, or for type coercion */
typedef struct dynamic_parse_state
{
    Size size;
    struct dynamic_parse_state *next;
    /*
     * This holds the last append_value scalar copy or the last append_element
     * scalar copy - it can only be one of the two. It is needed because when
     * an object or list is built, the upper level object or list will get a
     * copy of the result value on close. Our routines modify the value after
     * close and need this to update that value if necessary. Which is the
     * case for some casts.
     */
    dynamic_value *last_updated_value;
    dynamic_value cont_val;
} dynamic_parse_state;

/*
 * dynamic_iterator holds details of the type for each iteration. It also stores
 * an dynamic varlena buffer, which can be directly accessed in some contexts.
 */
typedef enum
{
    GTI_ARRAY_START,
    GTI_ARRAY_ELEM,
    GTI_OBJECT_START,
    GTI_OBJECT_KEY,
    GTI_OBJECT_VALUE
} gt_iterator_state;

typedef struct dynamic_iterator
{
    dynamic_container *container; // Container being iterated
    uint32 num_elems;            // Number of elements in children array (will be num_pairs for objects)
    bool is_scalar;              // Pseudo-array scalar value?
    gtentry *children;          // gtentrys for child nodes
    char *data_proper;           // Data proper. This points to the beginning of the variable-length data
    int curr_index;              // Current item in buffer (up to num_elems)
    uint32 curr_data_offset;     // Data offset corresponding to current item
    /*
     * If the container is an object, we want to return keys and values
     * alternately; so curr_data_offset points to the current key, and
     * curr_value_offset points to the current value.
     */
    uint32 curr_value_offset;
    gt_iterator_state state;    // Private state
    struct dynamic_iterator *parent;
} dynamic_iterator;

/* dynamic parse state */
typedef struct dynamic_in_state {
    dynamic_parse_state *parse_state;
    dynamic_value *res;
} dynamic_in_state;

/* Support functions */
int reserve_from_buffer(StringInfo buffer, int len);
short pad_buffer_to_int(StringInfo buffer);
uint32 get_dynamic_offset(const dynamic_container *agtc, int index);
uint32 get_dynamic_length(const dynamic_container *agtc, int index);
int compare_dynamic_containers_orderability(dynamic_container *a, dynamic_container *b);
dynamic_value *find_dynamic_value_from_container(dynamic_container *container, uint32 flags, const dynamic_value *key);
dynamic_value *get_ith_dynamic_value_from_container(dynamic_container *container, uint32 i);
dynamic_value *push_dynamic_value(dynamic_parse_state **pstate, dynamic_iterator_token seq, dynamic_value *agtval);
dynamic_iterator *dynamic_iterator_init(dynamic_container *container);
dynamic_iterator_token dynamic_iterator_next(dynamic_iterator **it, dynamic_value *val, bool skip_nested);
dynamic *dynamic_value_to_dynamic(dynamic_value *val);
bool dynamic_deep_contains(dynamic_iterator **val, dynamic_iterator **m_contained);
void dynamic_hash_scalar_value(const dynamic_value *scalar_val, uint32 *hash);
void dynamic_hash_scalar_value_extended(const dynamic_value *scalar_val, uint64 *hash, uint64 seed);
Datum get_numeric_datum_from_dynamic_value(dynamic_value *agtv);
bool is_numeric_result(dynamic_value *lhs, dynamic_value *rhs);


char *dynamic_to_cstring(StringInfo out, dynamic_container *in, int estimated_len);
char *dynamic_to_cstring_indent(StringInfo out, dynamic_container *in, int estimated_len);

Datum dynamic_from_cstring(char *str, int len);
bool is_dynamic_numeric(dynamic *agt);

size_t check_string_length(size_t len);
Datum integer_to_dynamic(int64 i);
Datum float_to_dynamic(float8 f);
Datum string_to_dynamic(char *s);
Datum boolean_to_dynamic(bool b);
void uniqueify_dynamic_object(dynamic_value *object);
char *dynamic_value_type_to_string(enum dynamic_value_type type);
bool is_decimal_needed(char *numstr);
int compare_dynamic_scalar_values(dynamic_value *a, dynamic_value *b);
dynamic_value *alter_property_value(dynamic *properties, char *var_name, dynamic *new_v, bool remove_property);
dynamic *get_one_dynamic_from_variadic_args(FunctionCallInfo fcinfo, int variadic_offset, int expected_nargs);
Datum column_get_datum(TupleDesc tupdesc, HeapTuple tuple, int column, const char *attname, Oid typid, bool isnull);
dynamic_value *get_dynamic_value(char *funcname, dynamic *agt_arg, enum dynamic_value_type type, bool error);
bool is_dynamic_null(dynamic *agt_arg);
void array_to_dynamic_internal(Datum array, dynamic_in_state *result);
Datum dynamic_to_float8(PG_FUNCTION_ARGS);

#define DYNAMICOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("dynamic"), ObjectIdGetDatum(postgraph_namespace_id())))

#define DYNAMICARRAYOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("_dynamic"), ObjectIdGetDatum(postgraph_namespace_id())))

Datum dynamic_object_field_impl(FunctionCallInfo fcinfo, dynamic *dynamic_in, char *key, int key_len, bool as_text);

void dynamic_put_escaped_value(StringInfo out, dynamic_value *scalar_val);

#endif
