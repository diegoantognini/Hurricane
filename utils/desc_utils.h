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

#ifndef _DESC_UTILS_
#define _DESC_UTILS_

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "boost_log_wrapper.h"

extern boost::property_tree::ptree pt;
extern boost::property_tree::ptree pt_hurricane;

static void init_hurricane_desc() {
  try {
    boost::property_tree::ini_parser::read_ini("hurricane.ini", pt_hurricane);
  }
  catch (...) {
    BOOST_LOG_TRIVIAL(fatal) << "Unable to read hurricane property file";
    exit(-1);
  }
}

#endif
