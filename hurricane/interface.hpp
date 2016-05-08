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

#ifndef _STORE_INTERFACE_
#define _STORE_INTERFACE_

#include "../utils/desc_utils.h"
#include "../utils/options_utils.h"
#include "io/io.hpp"
#include <boost/random.hpp>
#include <boost/thread.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include "client/client_fill.hpp"
#include "client/client_drain.hpp"
#include "client/client_barrier.hpp"
#include "server/server_async.hpp"
#include "server/server_sync.hpp"
#include <string>
#include <unordered_map>

using std::unordered_map;
using std::string;

// Interface for the slipstorage service

namespace hurricane {
  extern void *zmq_context;
  extern void *zmq_context_data_out;
  extern void *zmq_context_data_in;
  extern boost::asio::io_service ioService;
  extern boost::thread_group threadpool;
  extern boost::asio::io_service dioService;
  extern boost::thread_group diopool;

  extern busy_counter *have_outstanding_request;

  class client_fill;
  class client_drain;
  class client_barrier;

  extern client_fill *hurricane_client_fill;
  extern client_drain *hurricane_client_drain;
  extern client_barrier *hurricane_client_barrier;

  class server;
  extern server *hurricane_server;
  extern boost::thread *server_thread;

  static
  void init(unordered_map<string, hurricane::io*>* disks, bool RPC = false, bool with_server = true) {
    unsigned long zmq_threads = vm["zmq_threads"].as < unsigned long > ();
    BOOST_LOG_TRIVIAL(info) << "HURRICANE::ZMQ_THREADS " << zmq_threads;
    zmq_context = zmq_init(zmq_threads);
    zmq_context_data_out = zmq_init(zmq_threads);
    zmq_context_data_in = zmq_init(zmq_threads);
    baseport = vm["baseport"].as < unsigned long > ();

    if(!RPC) {
      hurricane_client_fill = new client_fill();
      hurricane_client_drain = new client_drain();
      hurricane_client_barrier = new client_barrier();
    }
    have_outstanding_request = new busy_counter();

    if(with_server)
      if (vm.count("use_async_server") > 0) {
        hurricane_server = new server_async(disks);
        BOOST_LOG_TRIVIAL(info) << "HURRICANE::SERVER ASYNC";
      } else {
        hurricane_server = new server_sync(disks);
        BOOST_LOG_TRIVIAL(info) << "HURRICANE::SERVER SYNC";
      }

    // Launch server thread
    if(with_server)
      server_thread = new boost::thread(boost::ref(*hurricane_server));
    
    if(!RPC) {
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::MACHINES " << hurricane_client_fill->get_machines();
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::ME " << hurricane_client_fill->get_me();
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::SLIPCHUNK " << hurricane_client_barrier->get_slipchunk();
    }
    BOOST_LOG_TRIVIAL(info) << "HURRICANE::QUOTA " << quota;

    // Launch async client thread
    threadpool.create_thread(boost::bind(&boost::asio::io_service::run,
                                         &ioService));
    //Launch async dio thread if needed
    if (vm.count("use_direct_io") > 0) {
      diopool.create_thread(boost::bind(&boost::asio::io_service::run,
                                        &dioService));
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::IO DIRECT";
    }
    else if (vm.count("use_dummy_io") > 0) {
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::IO DUMMY";
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::IO PAGECACHE";
    }
  }

  static
  void shutdown(bool RPC = false, bool with_server = true) {
    // Shut down async client service
    ioService.stop();
    threadpool.join_all();
    // Shut down dio service
    if (vm.count("use_direct_io") > 0) {
      dioService.stop();
      diopool.join_all();
    }
    if(!RPC){
      // Terminate server
      hurricane_req_t req;
      req.cmd = CMD_TERMINATE;
      hurricane_client_drain->access_store(&req,
                                           NULL,
                                           hurricane_client_drain->get_me());
    }
    if(with_server)
      server_thread->join();
    
    if(!RPC) {
      // Teardown sockets
      hurricane_client_drain->teardown_conn();
      hurricane_client_fill->teardown_conn();
      hurricane_client_barrier->teardown_conn();
    }
    // Terminate context
    zmq_term(zmq_context);
    zmq_term(zmq_context_data_out);
    zmq_term(zmq_context_data_in);
    if(!RPC) {
      hurricane_client_fill->print_times();
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::CLIENT::DISK_BYTES_READ " <<
      hurricane_client_fill->get_disk_bytes_read();
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::CLIENT::MEM_BYTES_READ " <<
      hurricane_client_fill->get_mem_bytes_read();
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::CLIENT::BYTES_WRITTEN " <<
      hurricane_client_drain->get_bytes_written();
    }
    have_outstanding_request->counter.print("HURRICANE::CLIENT_BUSY ");
  }
}

#endif

