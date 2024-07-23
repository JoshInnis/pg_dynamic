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
 * Declarations for dynamic parser.
 */

#ifndef POSTGRAPH_DYNAMIC_PARSER_H
#define POSTGRAPH_DYNAMIC_PARSER_H

#include "lib/stringinfo.h"

typedef enum
{
    DYNAMIC_TOKEN_INVALID,
    DYNAMIC_TOKEN_STRING,
    DYNAMIC_TOKEN_INTEGER,
    DYNAMIC_TOKEN_FLOAT,
    DYNAMIC_TOKEN_NUMERIC,
    DYNAMIC_TOKEN_TIMESTAMP,
    DYNAMIC_TOKEN_TIMESTAMPTZ,
    DYNAMIC_TOKEN_DATE,
    DYNAMIC_TOKEN_TIME,
    DYNAMIC_TOKEN_TIMETZ,
    DYNAMIC_TOKEN_INTERVAL,
    DYNAMIC_TOKEN_BOX,
    DYNAMIC_TOKEN_INET,
    DYNAMIC_TOKEN_CIDR,
    DYNAMIC_TOKEN_MACADDR,
    DYNAMIC_TOKEN_MACADDR8,
    DYNAMIC_TOKEN_OBJECT_START,
    DYNAMIC_TOKEN_OBJECT_END,
    DYNAMIC_TOKEN_ARRAY_START,
    DYNAMIC_TOKEN_ARRAY_END,
    DYNAMIC_TOKEN_COMMA,
    DYNAMIC_TOKEN_COLON,
    DYNAMIC_TOKEN_ANNOTATION,
    DYNAMIC_TOKEN_IDENTIFIER,
    DYNAMIC_TOKEN_TRUE,
    DYNAMIC_TOKEN_FALSE,
    DYNAMIC_TOKEN_NULL,
    DYNAMIC_TOKEN_END
} dynamic_token_type;

/*
 * All the fields in this structure should be treated as read-only.
 *
 * If strval is not null, then it should contain the de-escaped value
 * of the lexeme if it's a string. Otherwise most of these field names
 * should be self-explanatory.
 *
 * line_number and line_start are principally for use by the parser's
 * error reporting routines.
 * token_terminator and prev_token_terminator point to the character
 * AFTER the end of the token, i.e. where there would be a nul byte
 * if we were using nul-terminated strings.
 */
typedef struct dynamic_lex_context
{
    char *input;
    int input_length;
    char *token_start;
    char *token_terminator;
    char *prev_token_terminator;
    dynamic_token_type token_type;
    int lex_level;
    int line_number;
    char *line_start;
    StringInfo strval;
} dynamic_lex_context;

typedef void (*dynamic_struct_action)(void *state);
typedef void (*dynamic_ofield_action)(void *state, char *fname, bool isnull);
typedef void (*dynamic_aelem_action)(void *state, bool isnull);
typedef void (*dynamic_scalar_action)(void *state, char *token, dynamic_token_type tokentype, char *annotation);
typedef void (*dynamic_annotation_actions)(void *state, char *anotation);

/*
 * Semantic Action structure for use in parsing dynamic.
 * Any of these actions can be NULL, in which case nothing is done at that
 * point, Likewise, semstate can be NULL. Using an all-NULL structure amounts
 * to doing a pure parse with no side-effects, and is therefore exactly
 * what the dynamic input routines do.
 *
 * The 'fname' and 'token' strings passed to these actions are palloc'd.
 * They are not free'd or used further by the parser, so the action function
 * is free to do what it wishes with them.
 */
typedef struct dynamic_sem_action
{
    void *semstate;
    dynamic_struct_action object_start;
    dynamic_struct_action object_end;
    dynamic_struct_action array_start;
    dynamic_struct_action array_end;
    dynamic_ofield_action object_field_start;
    dynamic_ofield_action object_field_end;
    dynamic_aelem_action array_element_start;
    dynamic_aelem_action array_element_end;
    dynamic_scalar_action scalar;
    dynamic_annotation_actions annotation;
} dynamic_sem_action;

/*
 * parse_dynamic will parse the string in the lex calling the
 * action functions in sem at the appropriate points. It is
 * up to them to keep what state they need in semstate. If they
 * need access to the state of the lexer, then its pointer
 * should be passed to them as a member of whatever semstate
 * points to. If the action pointers are NULL the parser
 * does nothing and just continues.
 */
void parse_dynamic(dynamic_lex_context *lex, dynamic_sem_action *sem);

/*
 * constructors for dynamic_lex_context, with or without strval element.
 * If supplied, the strval element will contain a de-escaped version of
 * the lexeme. However, doing this imposes a performance penalty, so
 * it should be avoided if the de-escaped lexeme is not required.
 *
 * If you already have the dynamic as a text* value, use the first of these
 * functions, otherwise use ag_make_dynamic_lex_context_cstring_len().
 */
dynamic_lex_context *make_dynamic_lex_context(text *t, bool need_escapes);
dynamic_lex_context *make_dynamic_lex_context_cstring_len(char *str, int len,
                                                        bool need_escapes);

/*
 * Utility function to check if a string is a valid dynamic number.
 *
 * str argument does not need to be null-terminated.
 */
extern bool is_valid_dynamic_number(const char *str, int len);

extern char *dynamic_encode_date_time(char *buf, Datum value, Oid typid);

#endif
