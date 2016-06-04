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

#ifndef _LZ4_IO_
#define _LZ4_IO_

#include "hurricane_io.hpp"
#include <fcntl.h> 
#include "../../utils/lz4_mt.h"
/*
#include "../config.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
*/
#include <boost/asio/io_service.hpp>
//#include <boost/bind.hpp>

#define MAGICNUMBER_SIZE    4
namespace hurricane {
  const unsigned long flush_interval = (536870912);

  class sync_pagecache_lz4_io : public hurricane_io {
      unsigned long seen_bytes;
      unsigned long data_bytes;
      char name[100];
      bool snapped;
      LZ4F_decompressionContext_t uncomp_ctx;
      int fd_in;
      int fd_out;
      unsigned long nextbytes;
      unsigned long comp_array_size;
      lz4_compression_work *comp_array;
      unsigned long comp_head;
      unsigned long comp_tail;
      boost::asio::io_service lz4_comp_service;
      boost::asio::io_service::work service_wk;
      boost::thread_group lz4_comp_pool;
      lz4_uncompression_work *uncomp_array;
      unsigned long uncomp_array_size;
      unsigned long uncomp_ptr;
      boost::asio::io_service lz4_uncomp_service;
      boost::asio::io_service::work service_wk_uncomp;
      boost::thread_group lz4_uncomp_pool;

      unsigned long last_flush;

      void setup_compression(bool truncate = false) {
        fd_out = open(name, O_RDWR | O_LARGEFILE | O_TRUNC | O_CREAT, S_IRWXU);
        if (fd_out == -1) {
          BOOST_LOG_TRIVIAL(fatal) << "Unable to open file";
          exit(-1);
        }
        if (truncate) {
          rewind_file(fd_out);
          truncate_file(fd_out);
        }
        for (unsigned long i = 0; i < comp_array_size; i++) {
          comp_array[i].fd_out = fd_out;
          comp_array[i].startup(i == 0);
        }
        comp_head = comp_tail = 0;
        fd_in = -1;
      }

      void setup_decompression() {
        fd_in = open(name, O_RDONLY | O_LARGEFILE);
        if (fd_in == -1) {
          BOOST_LOG_TRIVIAL(fatal) << "Unable to open file";
          exit(-1);
        }
        fd_out = -1;
        for (unsigned long i = 0; i < uncomp_array_size; i++) {
          uncomp_array[i].fd_in = fd_in;
        }
        unsigned char header[11];
        read_from_file(fd_in, header, 11);
        nextbytes = uncomp_array[0].startup(header);
        for (unsigned long i = 1; i < uncomp_array_size; i++) {
          (void) uncomp_array[i].startup(header);
        }
        for (unsigned long i = 0; i < uncomp_array_size; i++) {
          nextbytes = uncomp_array[i].fill(nextbytes);
          uncomp_array[i].busy = true;
          lz4_uncomp_service.post(boost::bind(&lz4_uncompression_work::uncompress,
                                              &uncomp_array[i]));
        }
        uncomp_ptr = 0;
      }

      void destroy_all() {
        last_flush = 0;
        if (fd_in != -1) {
          for (unsigned long i = 0; i < uncomp_array_size; i++) {
            uncomp_array[i].shutdown();
          }
          close(fd_in);
          fd_in = -1;
        }

        if (fd_out != -1) {
          while (comp_tail != comp_head) {
            while (comp_array[comp_tail].busy);
            write_to_file(fd_out,
                            comp_array[comp_tail].comp_data,
                            comp_array[comp_tail].outbytes);
            comp_array[comp_tail].shutdown(false);
            comp_tail = (comp_tail + 1) % comp_array_size;
          }
          if (comp_array[comp_head].uncomp_bytes > 0) {
            comp_array[comp_head].compress();
            write_to_file(comp_array[comp_head].fd_out,
                          comp_array[comp_head].comp_data,
                          comp_array[comp_head].outbytes);
          }
          comp_array[comp_head].shutdown(true);
          close(fd_out);
          fd_out = -1;
        }
      }

  public:
      sync_pagecache_lz4_io(const char *name_in,
                            unsigned long ucomp_size = 0)
          : snapped(false),
            service_wk(lz4_comp_service),
            service_wk_uncomp(lz4_uncomp_service) {
        comp_array_size = vm["lz4_compress_threads"].as < unsigned long > ();
        uncomp_array_size = vm["lz4_decompress_threads"].as < unsigned long > ();
        BOOST_LOG_TRIVIAL(info) << "LZ4::COMPRESS:THREADS " << comp_array_size;
        BOOST_LOG_TRIVIAL(info) << "LZ4::DECOMPRESS:THREADS " << uncomp_array_size;
        comp_array = new lz4_compression_work[comp_array_size];
        uncomp_array = new lz4_uncompression_work[uncomp_array_size];
        for (unsigned long i = 0; i < comp_array_size; i++) {
          lz4_comp_pool.create_thread(boost::bind(&boost::asio::io_service::run,
                                                  &lz4_comp_service));
        }

        for (unsigned long i = 0; i < uncomp_array_size; i++) {
          lz4_uncomp_pool.create_thread(boost::bind(&boost::asio::io_service::run,
                                                    &lz4_uncomp_service));
        }

        strcpy(name, name_in);
        data_bytes = 0;
        seen_bytes = 0;
        setup_compression();

        last_flush = 0;
      }

      virtual unsigned long fill(unsigned char *buffer,
                                 unsigned long size) {
        unsigned long supplied_bytes = 0;
        while ((seen_bytes < data_bytes) && (size > 0)) {
          while (true) { // locate data to read
            while (uncomp_array[uncomp_ptr].busy);
            if (uncomp_array[uncomp_ptr].uncomp_bytes == 0) {
              nextbytes = uncomp_array[uncomp_ptr].fill(nextbytes);
              uncomp_array[uncomp_ptr].busy = true;
              lz4_uncomp_service.post(boost::bind(&lz4_uncompression_work::uncompress,
                                                  &uncomp_array[uncomp_ptr]));
              uncomp_ptr = (uncomp_ptr + 1) % uncomp_array_size;
            }
            else {
              break;
            }
          }
          unsigned long tocopy;

          tocopy = (uncomp_array[uncomp_ptr].uncomp_bytes > size) ? size : uncomp_array[uncomp_ptr].uncomp_bytes;
          memcpy(buffer, uncomp_array[uncomp_ptr].uncomp_data + uncomp_array[uncomp_ptr].uncomp_offset, tocopy);
          buffer += tocopy;
          uncomp_array[uncomp_ptr].uncomp_offset += tocopy;
          uncomp_array[uncomp_ptr].uncomp_bytes -= tocopy;
          size -= tocopy;
          seen_bytes += tocopy;
          supplied_bytes += tocopy;
        }

        return supplied_bytes;
      }

      virtual unsigned long seek_and_fill(unsigned char *buffer,
                                          unsigned long offset,
                                          unsigned long size) {
        BOOST_LOG_TRIVIAL(fatal) << "seek and fill not possible with lz4";
        exit(-1);
      }

      virtual void drain(unsigned char *buffer,
                         unsigned long size) {
        while (size > 0) {
          if (comp_array[comp_head].uncomp_bytes == 4194304) {
            comp_array[comp_head].busy = true;
            lz4_comp_service.post(boost::bind(&lz4_compression_work::compress,
                                              &comp_array[comp_head]));
            comp_head = (comp_head + 1) % comp_array_size;
            if (comp_head == comp_tail) {
              while (comp_array[comp_tail].busy);
            write_to_file(fd_out,
                            comp_array[comp_tail].comp_data,
                            comp_array[comp_tail].outbytes);
              comp_tail = (comp_tail + 1) % comp_array_size;
            }
          }
          unsigned long tocopy = (size > (4194304 - comp_array[comp_head].uncomp_bytes)) ? (4194304 - comp_array[comp_head].uncomp_bytes) : size;
          memcpy(comp_array[comp_head].uncomp_data + comp_array[comp_head].uncomp_offset, buffer, tocopy);
          buffer += tocopy;
          size -= tocopy;
          comp_array[comp_head].uncomp_offset += tocopy;
          comp_array[comp_head].uncomp_bytes += tocopy;
          seen_bytes += tocopy;
          data_bytes += tocopy;
        }
        fsync(fd_out);
      }

      virtual bool eof() {
        return seen_bytes == data_bytes;
      }

      virtual unsigned long size() {
        return data_bytes;
      }

      virtual unsigned long left() {
        return data_bytes - seen_bytes;
      }

      virtual void rewind() {
        //destroy_all();
        //setup_decompression();
        //seen_bytes = 0;
      }

      virtual void trunc() {
        //destroy_all();
        setup_compression(true);
        seen_bytes = 0;
        data_bytes = 0;
      }

      virtual ~sync_pagecache_lz4_io() {
        //destroy_all();
        lz4_comp_service.stop();
        lz4_comp_pool.join_all();
        lz4_uncomp_service.stop();
        lz4_uncomp_pool.join_all();
        delete[] comp_array;
        delete[] uncomp_array;
      }
  };
}
#endif