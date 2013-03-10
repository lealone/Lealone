
########################################################################
#
# Copyright (c) 2011 Yahoo! Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.
#
########################################################################

OSTYPE=$(shell uname -s)

SOURCESDIR=.
OBJECTSDIR=../../../target/main/native
LIBDIR=../../../lib

CC=g++

# This sets the name of the generated lib depending on the OS
TSOCOMMITHASHMAP=libtso-commithashmap.so
ifeq ($(OSTYPE),Darwin)
TSOCOMMITHASHMAP=libtso-commithashmap.jnilib
endif

INCLUDES=
ifeq ($(OSTYPE),Darwin) 
INCLUDES=-I/System/Library/Frameworks/JavaVM.framework/Headers 
else
INCLUDES=-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
endif
# The following includes the target dir where maven antrun places the .h file
INCLUDES+=-I$(OBJECTSDIR)

CFLAGS=-fPIC -O3 $(INCLUDES) -c
LDFLAGS=-fPIC -lstdc++ -shared
ifneq ($(OSTYPE),Darwin)
LDFLAGS+=-Wl,-soname=$(TSOCOMMITHASHMAP)
endif

SOURCES=$(shell find '$(SOURCESDIR)' -type f -name '*.cc')
OBJECTS=$(SOURCES:$(SOURCESDIR)/%.cc=$(OBJECTSDIR)/%.o)
LIB=$(LIBDIR)/$(TSOCOMMITHASHMAP)

all: $(LIB)

$(LIB): $(OBJECTS)
	-mkdir $(LIBDIR)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@
    
$(OBJECTS): $(SOURCES)
	mkdir -p $(OBJECTSDIR)
	$(CC) $(CFLAGS) $< -o $@
	
clean: 
	rm -f $(LIB)
