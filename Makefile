OBJECT_DIR= object_files
UTILS_SRC= utils.o
LIBS_SRC= core.o $(UTILS_SRC)
LIBS= $(addprefix $(OBJECT_DIR)/, $(LIBS_SRC))
UTILS= $(addprefix $(OBJECT_DIR)/, $(UTILS_SRC))
PROGS_SRC= driver.o 
PROGS= $(addprefix $(OBJECT_DIR)/, $(PROGS_SRC))
TARGETS= bin/slipbench


all: $(LIBS) $(TARGETS)

CXX?= g++

CXXFLAGS?= -O3 -DNDEBUG -Wall -Wno-unused-function -L/usr/local/lib
#CXXFLAGS?= -O3 -g -Wall -Wno-unused-function
#CXXFLAGS?= -g
CXXFLAGS += -Wfatal-errors
EXTRA_INCLUDES=-include core/types.h
EXTRA_INCLUDES+=-I/usr/local/include/boost-numeric-bindings

#ZLIB SPEEDS
#CXXFLAGS += -DZLIB_COMPRESSION_LEVEL=Z_DEFAULT_COMPRESSION
CXXFLAGS += -DZLIB_COMPRESSION_LEVEL=Z_BEST_SPEED
#CXXFLAGS += -DZLIB_COMPRESSION_LEVEL=Z_BEST_COMPRESSION

#ISA extensions
CXXFLAGS += -msse4.2
#Note avx2 is untested
#CXXFLAGS += -mavx2

#System libraries
SYSLIBS = -lboost_system -lboost_program_options -lboost_thread -lpthread -lz -lrt -lzmq
# Uncomment for ALS
SYSLIBS += -llapack

#Uncomment if you want low latency control messages
#CXXFLAGS += -DLOW_LATENCY_CONTROL

#Uncomment if you want periodic flushes
#CXXFLAGS += -DFLUSH_PERIODIC

-include $(LIBS:.o=.d) $(PROGS:.o=.d) 

$(OBJECT_DIR)/core.o:core/core.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/core.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/utils.o:utils/utils.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/utils.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/slipbench.o:utils/slipbench.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/slipbench.o' $< > $(@:.o=.d)

bin/slipbench:$(OBJECT_DIR)/slipbench.o 
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

clean:
	rm -f $(TARGETS) $(LIBS) $(OBJECT_DIR)/*
