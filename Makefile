OBJECT_DIR= object_files
UTILS_SRC= utils.o gIODFS.o gIODFS_types.o
LIBS_SRC= $(UTILS_SRC)
LIBS= $(addprefix $(OBJECT_DIR)/, $(LIBS_SRC))
UTILS= $(addprefix $(OBJECT_DIR)/, $(UTILS_SRC))
PROGS_SRC= 
PROGS= $(addprefix $(OBJECT_DIR)/, $(PROGS_SRC))
TARGETS= bin/slipbench bin/slipbench_rpc bin/iodfs bin/iodfs_tester_static bin/iodfs_tester_dynamic bin/block_mem


all: $(LIBS) $(TARGETS)

CXX?= g++

CXXFLAGS?= -O3 -DNDEBUG -Wall -Wno-unused-function -L/usr/local/lib
#CXXFLAGS?= -O3 -g -Wall -Wno-unused-function
#CXXFLAGS?= -g
CXXFLAGS+= -Wfatal-errors -std=c++11
#EXTRA_INCLUDES=-include core/types.h
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
SYSLIBS = -lboost_system -lboost_program_options -lboost_thread -lpthread -lz -lrt -lzmq -lthrift
# Uncomment for ALS
SYSLIBS += -llapack

#Uncomment if you want low latency control messages
#CXXFLAGS += -DLOW_LATENCY_CONTROL

#Uncomment if you want periodic flushes
#CXXFLAGS += -DFLUSH_PERIODIC

-include $(LIBS:.o=.d) $(PROGS:.o=.d) 

$(OBJECT_DIR)/utils.o:utils/utils.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -std=c++11 -MM -MT '$(OBJECT_DIR)/utils.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/slipbench.o:utils/slipbench.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -std=c++11 -MM -MT '$(OBJECT_DIR)/slipbench.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/block_mem.o:utils/block_mem.cpp
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -MM -MT '$(OBJECT_DIR)/block_mem.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/gIODFS.o: RPC/gen-cpp/IODFS.cpp
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -std=c++11 -MM -MT '$(OBJECT_DIR)/gIODFS.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/gIODFS_types.o: RPC/gen-cpp/IODFS_types.cpp
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -std=c++11 -MM -MT '$(OBJECT_DIR)/gIODFS_types.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/iodfs.o:RPC/iodfs.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -std=c++11 -MM -MT '$(OBJECT_DIR)/iodfs.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/slipbench_rpc.o:utils/slipbench_rpc.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -std=c++11 -MM -MT '$(OBJECT_DIR)/slipbench_rpc.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/iodfs_tester_static.o:RPC/iodfs_tester_static.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -std=c++11 -MM -MT '$(OBJECT_DIR)/iodfs_tester_static.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/iodfs_tester_dynamic.o:RPC/iodfs_tester_dynamic.cpp 
	$(CXX) $(CXXFLAGS) $(EXTRA_INCLUDES) -c -o $@ $<
	$(CXX) -std=c++11 -MM -MT '$(OBJECT_DIR)/iodfs_tester_dynamic.o' $< > $(@:.o=.d)

bin/slipbench:$(OBJECT_DIR)/slipbench.o 
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/block_mem:$(OBJECT_DIR)/block_mem.o $(LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/slipbench_rpc:$(OBJECT_DIR)/slipbench_rpc.o 
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/iodfs:$(OBJECT_DIR)/iodfs.o 
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/iodfs_tester_static:$(OBJECT_DIR)/iodfs_tester_static.o 
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

bin/iodfs_tester_dynamic:$(OBJECT_DIR)/iodfs_tester_dynamic.o 
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS) $(SYSLIBS)

clean:
	rm -f $(TARGETS) $(LIBS) $(OBJECT_DIR)/*
