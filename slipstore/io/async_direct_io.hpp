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

#ifndef _ASYNC_DIRECT_IO_
#define _ASYNC_DIRECT_IO_

#include <boost/asio/io_service.hpp>
#include "slipstore_io.hpp"
#include "dio_buffer.hpp"

namespace slipstore {
  extern boost::asio::io_service dioService;
  extern boost::thread_group diopool;

  class async_direct_io : public slipstore_io {

      unsigned long seen_bytes;
      unsigned long data_bytes;
      char name[100];
      int fd;
      bool snapped;
      dio_buffer *iobuf;
  public:
      async_direct_io(const char *name_in, unsigned long dio_unit, bool test = false)
          : seen_bytes(0),
            snapped(false) {
        strcpy(name, name_in);
        
        if(test) {
          fd = open(name, O_RDWR | O_LARGEFILE | O_DIRECT, S_IRWXU);
          data_bytes = get_file_size(fd);
        }
        else {
          // This prevents us from over writing any snapshots
          unlink_file(name);
          fd = open(name,
                    O_RDWR | O_LARGEFILE | O_CREAT | O_TRUNC | O_DIRECT,
                    S_IRWXU);
          data_bytes = 0;
        }
        iobuf = new dio_buffer(fd, dio_unit);
      }

      virtual unsigned long fill(unsigned char *buffer,
                                 unsigned long size) {
        unsigned long remaining_bytes = data_bytes - seen_bytes;
        if (size > remaining_bytes) {
          size = remaining_bytes;
        }
        seen_bytes += size;
        while (iobuf->get_busy());
        iobuf->set_busy();
        slipstore::dioService.post(boost::bind(&dio_buffer::read,
                                               iobuf,
                                               buffer,
                                               size));
        while (iobuf->get_busy());
        return size;
      }

      virtual unsigned long seek_and_fill(unsigned char *buffer,
                                          unsigned long offset,
                                          unsigned long size) {
        unsigned long remaining_bytes;
        while (iobuf->get_busy());
        iobuf->set_busy();
        slipstore::dioService.post(boost::bind(&dio_buffer::seek,
                                               iobuf,
                                               offset));
        while (iobuf->get_busy());
        seen_bytes = offset;
        remaining_bytes = data_bytes - seen_bytes;
        if (size > remaining_bytes) {
          size = remaining_bytes;
        }
        seen_bytes += size;
        iobuf->set_busy();
        slipstore::dioService.post(boost::bind(&dio_buffer::read,
                                               iobuf,
                                               buffer,
                                               size));
        while (iobuf->get_busy());
        return size;
      }

      virtual void drain(unsigned char *buffer,
                         unsigned long size) {
        seen_bytes += size;
        while (iobuf->get_busy());
        iobuf->set_busy();
        slipstore::dioService.post(boost::bind(&dio_buffer::write,
                                               iobuf,
                                               buffer,
                                               size));
        while (iobuf->get_busy());
        data_bytes += size;
        usage += size;
        if (usage > max_quota_usage) {
          max_quota_usage = usage;
        }
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
        while (iobuf->get_busy());
        iobuf->set_busy();
        slipstore::dioService.post(boost::bind(&dio_buffer::flush,
                                               iobuf));
        while (iobuf->get_busy());
        rewind_file(fd);
        seen_bytes = 0;
      }

      virtual void trunc() {
        while (iobuf->get_busy());
        iobuf->set_busy();
        slipstore::dioService.post(boost::bind(&dio_buffer::flush,
                                               iobuf));
        while (iobuf->get_busy());
        if (!snapped) {
          rewind_file(fd);
          truncate_file(fd);
        }
        else {
          close(fd);
          unlink_file(name);
          fd = open(name,
                    O_RDWR | O_LARGEFILE | O_CREAT | O_TRUNC | O_DIRECT,
                    S_IRWXU);
          snapped = false;
        }
        usage -= data_bytes;
        seen_bytes = 0;
        data_bytes = 0;
      }

      virtual ~async_direct_io() {
        delete iobuf;
        close(fd);
      }
  };
}
#endif
