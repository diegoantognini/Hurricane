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

#include "../slipstore/memory_buffer.hpp"
#include "autotuner.hpp"

 namespace x_lib {
  unsigned long configuration::cached_partitions = 0;
  unsigned long configuration::partition_shift = 0;
  unsigned long configuration::tiles = 0;
  unsigned long configuration::tile_shift = 0;
  unsigned long configuration::super_partitions = 0;
  unsigned long configuration::super_partition_shift = 0;
  unsigned long configuration::ext_mem_bits = 0;
  unsigned long configuration::ext_fanout_bits = 0;
  unsigned long map_spshift_wrap::map_spshift = 100;

  unsigned long **memory_buffer::aux_index = NULL;
  unsigned char *memory_buffer::aux_buffer = NULL;
  memory_buffer *volatile memory_buffer::freelist = NULL;
  volatile unsigned long memory_buffer::freelist_lock = 0;

  bool memory_buffer::use_qsort = false;
  bool memory_buffer::do_batch_io = false;
  bool memory_buffer::gather_io_drain = false;
  unsigned long qsort_keys = 0;
}

namespace slipstore {
  slipstore::client_fill *slipstore_client_fill = NULL;
  slipstore::client_drain *slipstore_client_drain = NULL;
  slipstore::client_barrier *slipstore_client_barrier = NULL;
  slipstore::server *slipstore_server = NULL;
  slipstore::busy_counter *have_outstanding_request = NULL;
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
