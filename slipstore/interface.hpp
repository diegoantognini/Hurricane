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
#include <zmq.h>
#include <netinet/ip.h>
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include "helper/help_base.hpp"
#include "helper/help_skip.hpp"
#include "server/server.hpp"
#include "server/server_async.hpp"
#include "server/server_sync.hpp"
#include "client/client_fill.hpp"
#include "client/client_drain.hpp"
#include "client/client_barrier.hpp"
#include "busy_counter.hpp"

// Interface for the slipstorage service

namespace slipstore {
  extern void *zmq_context;
  extern void *zmq_context_data_out;
  extern void *zmq_context_data_in;
  extern unsigned long baseport;
  extern boost::asio::io_service ioService;
  extern boost::thread_group threadpool;
  extern boost::asio::io_service dioService;
  extern boost::thread_group diopool;

  const std::string protocol = "tcp";
  const std::string separator = ":";

  // Number of endpoints
  static unsigned long endpoint_count(unsigned long machines) {
    return machines + //server data in
           machines + //client data in
           1 + // server control
           1;  // barrier
  }

  // Utility functions to generate endpoints for connections
  static
  std::string conn_server_data_endpoint(unsigned long mc_target,
                                        unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://server";
    } else {
      addr << protocol << "://";
      addr << pt_slipstore.get<std::string>(name.str().c_str());
      unsigned long machines = pt_slipstore.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static
  std::string conn_server_data_bindpoint(unsigned long mc_target,
                                         unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://server";
    } else {
      addr << protocol << "://";
      addr << pt_slipstore.get<std::string>(name.str().c_str());
      unsigned long machines = pt_slipstore.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static
  std::string conn_client_data_endpoint(unsigned long mc_target,
                                        unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://client";
    } else {
      addr << protocol << "://";
      addr << pt_slipstore.get<std::string>(name.str().c_str());
      unsigned long machines = pt_slipstore.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           machines + mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static
  std::string conn_client_data_bindpoint(unsigned long mc_target,
                                         unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://client";
    } else {
      addr << protocol << "://";
      addr << pt_slipstore.get<std::string>(name.str().c_str());
      unsigned long machines = pt_slipstore.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           machines + mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static
  std::string conn_server_control_endpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    addr << protocol << "://";
    addr << pt_slipstore.get<std::string>(name.str().c_str());
    unsigned long machines = pt_slipstore.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines;
    addr << separator << port;
    return addr.str();
  }

  static
  std::string conn_server_control_bindpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    addr << protocol << "://";
    addr << pt_slipstore.get<std::string>(name.str().c_str());
    unsigned long machines = pt_slipstore.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines;
    addr << separator << port;
    return addr.str();
  }

  static
  std::string conn_client_barrier_endpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    addr << protocol << "://";
    addr << pt_slipstore.get<std::string>(name.str().c_str());
    unsigned long machines = pt_slipstore.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines + 1;
    addr << separator << port;
    return addr.str();
  }

  static
  std::string conn_client_barrier_bindpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    addr << protocol << "://";
    addr << pt_slipstore.get<std::string>(name.str().c_str());
    unsigned long machines = pt_slipstore.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines + 1;
    addr << separator << port;
    return addr.str();
  }

  static
  void slipstore_bind_endpoint(void *socket, const char *endpoint) {
    int rc = zmq_bind(socket, endpoint);
    if (rc != 0) {
      BOOST_LOG_TRIVIAL(fatal) <<
      "SLIPSTORE::COMM Unable to setup listening socket at "
      << endpoint;
      perror("zmq_bind:");
      exit(-1);
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::COMM Listening at "
      << endpoint;
    }
  }

  static
  void slipstore_connect_endpoint(void *socket, const char *endpoint) {
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::COMM Connecting to " <<
    endpoint;
    zmq_connect(socket, endpoint);
  }

  static
  void slipstore_mark_low_latency(void *socket) {
#ifdef LOW_LATENCY_CONTROL
    int tos_value = IPTOS_LOWDELAY;
    int ret = zmq_setsockopt(socket, ZMQ_TOS, &tos_value, sizeof(tos_value));
    if(ret == -1) {
      BOOST_LOG_TRIVIAL(warn) 
  << "SLIPSTORE: Unable to make socket low latency: "
  << strerror(errno);
    }
#endif
  }

  static
  unsigned long do_zmq_send(void *socket,
                            unsigned char *data,
                            unsigned long size,
                            const char *context) {
    while (true) {
      int rc = zmq_send(socket, data, size, 0);
      if (rc == -1) {
        if (errno != EAGAIN) {
          BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to transmit";
          perror(context);
          exit(-1);
        }
        // Retry
      }
      else {
        break;
      }
    }
    return size;
  }

  static
  unsigned long do_zmq_recv(void *socket,
                            unsigned char *data,
                            unsigned long size,
                            const char *context) {
    int rc;
    while (true) {
      rc = zmq_recv(socket, data, size, 0);
      if (rc == -1) {
        if (errno != EAGAIN) {
          BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to receive";
          perror(context);
          exit(-1);
        }
        // Retry
      }
      else {
        break;
      }
    }
    return (unsigned long) rc;
  }

  static
  unsigned long do_zmq_recv_noblock(void *socket,
                                    unsigned char *data,
                                    unsigned long size,
                                    const char *context) {
    int rc;
    rc = zmq_recv(socket, data, size, ZMQ_NOBLOCK);

    if (rc == -1 && errno != EAGAIN) {
      BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to receive";
      perror(context);
      exit(-1);
    }
    return (unsigned long) rc;
  }

  extern busy_counter *have_outstanding_request;

  extern client_fill *slipstore_client_fill;
  extern client_drain *slipstore_client_drain;

  extern client_barrier *slipstore_client_barrier;

  extern server *slipstore_server;
  extern boost::thread *server_thread;

  static
  void init(io *disks, unsigned long tile_size, unsigned long partitions) {
    unsigned long zmq_threads = vm["zmq_threads"].as < unsigned
    long > ();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::ZMQ_THREADS " <<
    zmq_threads;
    zmq_context = zmq_init(zmq_threads);
    zmq_context_data_out = zmq_init(zmq_threads);
    zmq_context_data_in = zmq_init(zmq_threads);
    baseport = vm["baseport"].as < unsigned
    long > ();
    slipstore_client_fill = new client_fill();
    slipstore_client_drain = new client_drain();
    slipstore_client_barrier = new client_barrier();
    have_outstanding_request = new busy_counter();

    if (vm.count("use_async_server") > 0) {
      slipstore_server = new server_async(disks, tile_size, partitions);
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::SERVER ASYNC";
    } else {
      slipstore_server = new server_sync(disks, tile_size, partitions);
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::SERVER SYNC";
    }

    // Launch server thread
    server_thread = new boost::thread(boost::ref(*slipstore_server));
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::MACHINES " <<
    slipstore_client_fill->get_machines();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::ME " <<
    slipstore_client_fill->get_me();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::SLIPCHUNK " <<
    slipstore_client_barrier->get_slipchunk();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::QUOTA " <<
    quota;
    // Launch async client thread
    threadpool.create_thread(boost::bind(&boost::asio::io_service::run,
                                         &ioService));
    //Launch async dio thread if needed
    if (vm.count("use_direct_io") > 0) {
      diopool.create_thread(boost::bind(&boost::asio::io_service::run,
                                        &dioService));
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::IO DIRECT";
    }
    else if (vm.count("use_dummy_io") > 0) {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::IO DUMMY";
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::IO PAGECACHE";
    }
  }

  static
  void shutdown() {
    // Shut down async client service
    ioService.stop();
    threadpool.join_all();
    // Shut down dio service
    if (vm.count("use_direct_io") > 0) {
      dioService.stop();
      diopool.join_all();
    }
    // Terminate server
    slipstore_req_t req;
    req.cmd = CMD_TERMINATE;
    slipstore_client_drain->access_store(&req,
                                         NULL,
                                         slipstore_client_drain->get_me());
    server_thread->join();
    // Teardown sockets
    slipstore_client_drain->teardown_conn();
    slipstore_client_fill->teardown_conn();
    slipstore_client_barrier->teardown_conn();
    // Terminate context
    zmq_term(zmq_context);
    zmq_term(zmq_context_data_out);
    zmq_term(zmq_context_data_in);
    slipstore_client_fill->print_times();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::CLIENT::DISK_BYTES_READ " <<
    slipstore_client_fill->get_disk_bytes_read();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::CLIENT::MEM_BYTES_READ " <<
    slipstore_client_fill->get_mem_bytes_read();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::CLIENT::BYTES_WRITTEN " <<
    slipstore_client_drain->get_bytes_written();
    have_outstanding_request->counter.print("SLIPSTORE::CLIENT_BUSY ");
  }
}

#endif

