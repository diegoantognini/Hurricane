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

#ifndef _STORE_IO_
#define _STORE_IO_
// Handle Slipstream Disk I/O
#include <unistd.h>
#include <sys/types.h>
#include "boost/multi_array.hpp"
#include "../../utils/memory_utils.h"
#include "../../utils/clock_utils.h"
#include "../../utils/options_utils.h"
#include "sync_pagecache_io.hpp"
#include "async_direct_io.hpp"
#include "dummy_io.hpp"

namespace slipstore {
  // Requests to slipstore service
  static const unsigned long CMD_FILL = 0;
  static const unsigned long CMD_SEEK_AND_FILL = 1;
  static const unsigned long CMD_DRAIN = 2;
  static const unsigned long CMD_EOF = 3;
  static const unsigned long CMD_NOP = 4;
  static const unsigned long CMD_TERMINATE = 7;
  static const unsigned long CMD_RESET = 9;
  // Responses from slipstore service
  static const unsigned long RESP_ACK = 0;
  static const unsigned long RESP_NACK = 1;

  struct slipstore_req_t {
      unsigned long source_mc;
      unsigned long cmd;
      unsigned long size;
      unsigned long offset; // Used only by seek and fill
      char filename[100];
  };

  class io {
      slipstore_io* buffer;
      char name[100];
      fsnap *fsnaps;
      unsigned char *membuffer;
      rtc_clock slipstore_io_time;
      rtc_clock slipstore_snap_time;

  public:
      io(const char* filename, unsigned char *membuffer_in)
          : fsnaps(NULL),
            membuffer(membuffer_in) {
        strcpy(name, filename);
        //unsigned long me = pt_slipstore.get < unsigned long > ("machines.me");

        if (vm.count("use_direct_io") > 0)
          buffer = new async_direct_io(filename, vm["dio_unit"].as < unsigned long > ());
        else if (vm.count("use_dummy_io") > 0)
          buffer = new dummy_io(filename);
        else 
          buffer = new sync_pagecache_io(filename);

        quota = vm["quota"].as < unsigned
        long > ();
        usage = 0;
        max_quota_usage = 0;
      }

      bool eof() {
        return buffer->eof();
      }

      unsigned long fill(unsigned char *buffer_in, slipstore_req_t *req) {
          unsigned long size;
          slipstore_io_time.start();
          size = buffer->fill(buffer_in, req->size);
          slipstore_io_time.stop();
          return size;
      }

      unsigned long seek_and_fill(unsigned char *buffer_in, slipstore_req_t *req) {
        unsigned long size;
        slipstore_io_time.start();
        size = buffer->seek_and_fill(buffer_in, req->offset, req->size);
        slipstore_io_time.stop();
        return size;
      }

      // This is how Amitabha writes code
      bool have_drain_space(slipstore_req_t *req) {
        if ((usage + req->size) <= quota) {
          return true;
        }
        else {
          return false;
        }
      }

      // This is how Laurent writes code
      bool have_drain_space(unsigned long additional_size) {
        return usage + additional_size <= quota;
      }

      void drain(unsigned char *buffer_in, slipstore_req_t *req) {
        slipstore_io_time.start();
        buffer->drain(buffer_in, req->size);
        slipstore_io_time.stop();
      }

      void rewind() {
        slipstore_io_time.start();
        buffer->rewind();
        slipstore_io_time.stop();
      }

      unsigned long get_size() {
        return buffer->size();
      }

      unsigned long get_bytes_left() {
        return buffer->left();
      }

      void trunc() {
        slipstore_io_time.start();
        buffer->trunc();
        slipstore_io_time.stop();
      }

      // For running fill speed test
      void fill_test_prep(const char* name_in = "fill_test_data") {
        delete buffer;
        strcpy(name, name_in);
        if (vm.count("use_direct_io") > 0)
          buffer = new async_direct_io(name, vm["slipchunk"].as < unsigned long > (), true);
        else if (vm.count("use_dummy_io") > 0) {
          dummy_io *dummy = new dummy_io(name, true);
          dummy->force_file_size(vm["slipbench_fsize"].as < unsigned long > ());
          buffer = dummy;
        }
        else
          buffer = new sync_pagecache_io(name, true);
      }

      ~io() {
        slipstore_io_time.print("SLIPSTORE::IO::TIME");
        slipstore_snap_time.print("SLIPSTORE::SNAP::TIME");
        BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::MAX_DISK_USAGE_BYTES "<< max_quota_usage;
        delete buffer;
      }
  };
}
#endif
