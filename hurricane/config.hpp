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

#ifndef _CONFIG_HURRICANE_
#define _CONFIG_HURRICANE_

#include "interface.hpp"
#include "client/client_fill.hpp"
#include "client/client_drain.hpp"
#include "client/client_barrier.hpp"
 
namespace hurricane {
  client_fill *hurricane_client_fill = NULL;
  client_drain *hurricane_client_drain = NULL;
  client_barrier *hurricane_client_barrier = NULL;
  server *hurricane_server = NULL;
  busy_counter *have_outstanding_request = NULL;
  boost::thread *server_thread = NULL;
  unsigned long baseport = 0;
  void *zmq_context = NULL;
  void *zmq_context_data_out = NULL;
  void *zmq_context_data_in = NULL;
  unsigned long quota = 0;
  unsigned long usage = 0;
  unsigned long max_quota_usage = 0;
  boost::asio::io_service ioService;
  boost::asio::io_service::work work(ioService);
  boost::thread_group threadpool;
  boost::asio::io_service dioService;
  boost::asio::io_service::work dio_work(dioService);
  boost::thread_group diopool;
};

#endif