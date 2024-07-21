# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

MODULE_big = pg_dynamic

OBJS = src/pg_dynamic.o \
       src/namespace.o \
       src/dynamic_io.o \
       src/typecasting.o \
       src/dynamic_integer.o \
       src/parser.o \
       src/ext.o \
       src/ops.o \
       src/util.o

EXTENSION = pg_dynamic

DATA = pg_dynamic--0.1.0.sql

# sorted in dependency order
REGRESS = dynamic \
          integer

srcdir=`pwd`

.PHONY:all

all: pg_dynamic--0.1.0.sql

ag_regress_dir = $(srcdir)/regress
REGRESS_OPTS = --load-extension=pg_dynamic --inputdir=$(ag_regress_dir) --outputdir=$(ag_regress_dir) --temp-instance=$(ag_regress_dir)/instance --port=64729 --encoding=UTF-8

ag_regress_out = instance/ log/ results/ regression.*
EXTRA_CLEAN = $(addprefix $(ag_regress_dir)/, $(ag_regress_out))

ag_include_dir = $(srcdir)/include

PG_CPPFLAGS = -w -I$(ag_include_dir) 
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

installcheck: export LC_COLLATE=C
