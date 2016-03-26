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

#ifndef _SYNC_PAGECACHE_IO_
#define _SYNC_PAGECACHE_IO_

#include "generic_io.hpp"

namespace slipstore {
  class sync_pagecache_io : public slipstore_io {
      unsigned long stream;
      unsigned long partition;
      unsigned long tile;
      unsigned long seen_bytes;
      unsigned long data_bytes;
      char name[100];
      int fd;
      bool snapped;
  public:
      sync_pagecache_io(unsigned long stream_in,
                        unsigned long partition_in,
                        unsigned long tile_in,
                        const char *name_in = NULL)
          : stream(stream_in),
            partition(partition_in),
            tile(tile_in),
            seen_bytes(0),
            snapped(false) {
        if (name_in != NULL) {
          strcpy(name, name_in);
          fd = open(name, O_RDWR | O_LARGEFILE, S_IRWXU);
          data_bytes = get_file_size(fd);
        }
        else {
          sprintf(name, "stream.%lu.%lu.%lu", stream, partition, tile);
          // This prevents us from over writing any snapshots
          unlink_file(name);
          fd = open(name,
                    O_RDWR | O_LARGEFILE | O_CREAT | O_TRUNC,
                    S_IRWXU);
          data_bytes = 0;
        }
        posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
      }

      virtual unsigned long fill(unsigned char *buffer,
                                 unsigned long size) {
        unsigned long remaining_bytes = data_bytes - seen_bytes;
        if (size > remaining_bytes) {
          size = remaining_bytes;
        }
        seen_bytes += size;
        read_from_file(fd, buffer, size);
        return size;
      }

      virtual unsigned long seek_and_fill(unsigned char *buffer,
                                          unsigned long offset,
                                          unsigned long size) {
        unsigned long remaining_bytes;
        set_filepos(fd, offset);
        seen_bytes = offset;
        remaining_bytes = data_bytes - seen_bytes;
        if (size > remaining_bytes) {
          size = remaining_bytes;
        }
        seen_bytes += size;
        read_from_file(fd, buffer, size);
        return size;
      }

      virtual void drain(unsigned char *buffer,
                         unsigned long size) {
        seen_bytes += size;
        write_to_file(fd, buffer, size);
#ifdef FLUSH_PERIODIC
        if(((data_bytes + size)/(4*1024*1024)) !=
     (data_bytes/(4*1024*1024))) {
    fdatasync(fd);
        }
#endif
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
        rewind_file(fd);
        seen_bytes = 0;
      }

      virtual void trunc() {
        if (!snapped) {
          rewind_file(fd);
          truncate_file(fd);
        }
        else {
          close(fd);
          unlink_file(name);
          fd = open(name,
                    O_RDWR | O_LARGEFILE | O_CREAT | O_TRUNC,
                    S_IRWXU);
          posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
          snapped = false;
        }
        usage -= data_bytes;
        seen_bytes = 0;
        data_bytes = 0;
      }

      virtual ~sync_pagecache_io() {
        close(fd);
      }
  };
}
#endif
