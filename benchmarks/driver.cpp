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

//! Main driver for benchmark programs
#include "../utils/boost_log_wrapper.h"
// Core
#ifdef PYTHON_SUPPORT
#include "../core/sg_driver_python.hpp"
#else

#include "../core/sg_driver.hpp"
//#include "../core/sg_driver_async.hpp"
#endif
// Types
#include "../format/type_1.hpp"
#include "../format/type_2.hpp"

int main(int argc, const char *argv[]) {
  /* Parse the cmd line */
  setup_options(argc, argv);

  BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " STARTUP";
  BOOST_LOG_TRIVIAL(info) << "CORE::GRAPH " << vm["graph"].as<std::string>();
  BOOST_LOG_TRIVIAL(info) << "CORE::ALGORITHM " << vm["benchmark"].as<std::string>();

  if (vm.count("use_vertex_striping") > 0) {
    BOOST_LOG_TRIVIAL(info) << "CORE::VERTEX_STRIPING ON";
  } else {
    BOOST_LOG_TRIVIAL(info) << "CORE::VERTEX_STRIPING OFF";
  }

  /* Read in key graph information */
  init_graph_desc(vm["graph"].as<std::string>());
  /* Read in slipstore information */
  init_slipstore_desc();

  unsigned long blocked_memory = vm["blocked_memory"].as < unsigned
  long > ();

  if (blocked_memory > 0) {
    BOOST_LOG_TRIVIAL(info) << clock::timestamp()
    << " Blocking memory amount "
    << blocked_memory;
    (void) map_anon_memory(blocked_memory, true, "Blockmem");
    BOOST_LOG_TRIVIAL(info) << clock::timestamp()
    << " Done blocking memory";
  }

  /* Only certain combinations of algorithms and formats
   * are supported. This is encoded below.
   * Algorithms that require the edge value cannot use format type2
   */
  if (false);
  
  else {
    BOOST_LOG_TRIVIAL(fatal) << "Don't know how to run " <<
    vm["benchmark"].as<std::string>() <<
    " on " << vm["graph"].as<std::string>();
    exit(-1);
  }
  BOOST_LOG_TRIVIAL(info) << "SHUTDOWN";
  return 0;
} 

