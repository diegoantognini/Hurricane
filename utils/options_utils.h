/*
 * Chaos 
 *
 * Copyright 2015 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Utilities for options
#ifndef _OPTIONS_UTILS_
#define _OPTIONS_UTILS_

#include <boost/program_options.hpp>
#include "boost_log_wrapper.h"

extern boost::program_options::options_description desc;
extern boost::program_options::variables_map vm;

static void setup_options(int argc, const char *argv[]) {
  desc.add_options()
      ("help,h", "Produce help message")
      ("processors,p", boost::program_options::value < unsigned
  long > ()->default_value(1),
      "Number of processors")
  ("cpu_cache_size",
       boost::program_options::value < unsigned
  long > ()->default_value(2097152),
      "Per-processor cache size")
  ("cpu_line_size",
      boost::program_options::value < unsigned
  long > ()->default_value(64),
      "Per-processor cacheline size")
  ("physical_memory",
      boost::program_options::value < unsigned
  long > ()->default_value(1073741824),
      "Physical memory available")
  ("blocked_memory",
      boost::program_options::value < unsigned
  long > ()->default_value(0),
      "Physical memory to block before starting the test");

  // Slipstore options
  desc.add_options()
      ("baseport",
       boost::program_options::value < unsigned
  long > ()->default_value(5000),
      "Starting port number")
  ("slipchunk",
      boost::program_options::value < unsigned
  long > ()->default_value(4194304),
      "I/O chunk size for slipstore")
  ("slipstore_servers",
      boost::program_options::value
      < unsigned
  long > ()->default_value(ULONG_MAX),
      "Number of slipstore servers to use")
  ("zmq_threads",
      boost::program_options::value < unsigned
  long > ()->default_value(2),
      "Number of zmq threads to use")
  ("use_direct_io", "Use Direct I/O")
      ("dio_unit",
       boost::program_options::value < unsigned
  long > ()->default_value(4194304),
      "DIO Unit, should be approx. same as slipchunk and 4K aligned")
  ("use_dummy_io", "Use Dummy I/O")
      ("use_async_server", "Use Asynchronous Server")
      ("polling_server", "Use polling server")
      ("polling_client", "Use polling client")
      ("use_vertex_striping", "Use Vertex Striping")
      ("use_memory_scatter", "Perform scatter helping from memory")
      ("centralized", "Centralized Server")
      ("log_phases", "Log timestamps for entry/exit of phases")
      ("request_batching", "Batch requests to storage servers")
      ("gather_io_drain", "Use gather IO for drains")
      ("batch_size",
       boost::program_options::value < unsigned
  long > ()->default_value(1),
      "Size of outstanding requests window");

  // slipbench
  desc.add_options()
      ("slipbench_fsize",
       boost::program_options::value
       < unsigned
  long > ()->default_value(4 * 1024 * 1024 * 1024UL),
      "file size in bytes for slipbench")
  ("slipbench_silence",
      boost::program_options::value
      < unsigned
  long > ()->default_value(ULONG_MAX),
      "clients to silence")
  ("drain_test", "Run drain test")
      ("fill_test", "Run fill test")
      ("slipbench_align",
       boost::program_options::value
       < unsigned
  long > ()->default_value(1UL),
      "Alignment for data transfers")
  ("slipbench_background_threads",
      boost::program_options::value
      < unsigned
  long > ()->default_value(0UL),
      "Number of background interference threads");

  try {
    boost::program_options::store(boost::program_options::parse_command_line(argc,
                                                                             argv,
                                                                             desc),
                                  vm);
    boost::program_options::notify(vm);
  }
  catch (boost::program_options::error &e) {
    if (vm.count("help") || argc == 1) {
      std::cerr << desc << "\n";
    }
    std::cerr << "Error:" << e.what() << std::endl;
    std::cerr << "Try: " << argv[0] << " --help" << std::endl;
    exit(-1);
  }
}

static void check_pow_2(unsigned long value, const char *fail_string) {
  if ((value & (value - 1)) != 0) {
    BOOST_LOG_TRIVIAL(fatal) << fail_string;
    exit(-1);
  }
}

#endif
