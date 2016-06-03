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

#ifndef _SYNC_PAGECACHE_MT_IO_
#define _SYNC_PAGECACHE_MT_IO_

#include "hurricane_io.hpp"
#include "cache.h"
#define FALLOC_FL_COLLAPSE_RANGE       0x08
#include <fcntl.h>

namespace hurricane {
  class sync_pagecache_mt_io : public hurricane_io {
      unsigned long seen_bytes;
      unsigned long data_bytes;
      char name[100];
      int fdr;
      int fdw;
      bool snapped;
  public:
      sync_pagecache_mt_io(const char *name_in = NULL)
          : seen_bytes(0),
            snapped(false) {
        strcpy(name, name_in);

        fdw = open(name, O_WRONLY | O_LARGEFILE | O_CREAT | O_TRUNC, S_IRWXU);

        if (fdw == -1) {
          perror("Error opening file for writing");
          exit(EXIT_FAILURE);
        }
        posix_fadvise(fdw, 0, 0, POSIX_FADV_SEQUENTIAL);

        fdr = open(name, O_RDWR | O_LARGEFILE, S_IRWXU);
        if (fdr == -1) {
          perror("Error opening file for reading");
          exit(EXIT_FAILURE);
        }
        posix_fadvise(fdr, 0, 0, POSIX_FADV_SEQUENTIAL);

        seen_bytes = 0;
        if (name_in != NULL) {
          data_bytes = get_file_size(fdr);
        } else {
          data_bytes = 0;
        }
      }

      virtual unsigned long fill(unsigned char *buffer,
                                 unsigned long size) {
        unsigned long remaining_bytes = data_bytes - seen_bytes;
        if (size > remaining_bytes) {
          size = remaining_bytes;
        }
        seen_bytes += size;
        read_from_file(fdr, buffer, size);
        return size;
      }

      virtual unsigned long seek_and_fill(unsigned char *buffer,
                                          unsigned long offset,
                                          unsigned long size) {
        BOOST_LOG_TRIVIAL(fatal) << "Seek and fill not possible";
        exit(-1);
      }

      virtual void drain(unsigned char *buffer,
                         unsigned long size) {
        seen_bytes += size;
        write_to_file(fdw, buffer, size);
#ifdef FLUSH_PERIODIC
        if(((data_bytes + size)/(4*1024*1024)) != (data_bytes/(4*1024*1024))) {
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
        rewind_file(fdr);
        seen_bytes = 0;
      }

      virtual void trunc() {
        rewind_file(fdr);
        rewind_file(fdw);
        truncate_file(fdw);
        usage -= data_bytes;
        seen_bytes = 0;
        data_bytes = 0;
      }

      virtual void collapse_range(off_t offset, off_t len) {
        off_t oldfppos = lseek(fdr, 0, SEEK_CUR);
        int res = fallocate(fdr, FALLOC_FL_COLLAPSE_RANGE, offset, len);
        if (res == -1) {
          perror("Error collapsing file range");
          exit(EXIT_FAILURE);
        }
        // Reset file pointers to correct position
        off_t newfppos = oldfppos - len;
        res = lseek(fdr, newfppos, SEEK_SET);
        if (res == -1) {
          perror("Error adjusting read file pointer");
          exit(EXIT_FAILURE);
        }
        res = lseek(fdw, 0, SEEK_END);
        if (res == -1) {
          perror("Error adjusting write file pointer");
          exit(EXIT_FAILURE);
        }
      }

      virtual ~sync_pagecache_mt_io() {

      }
  };
}
#endif
