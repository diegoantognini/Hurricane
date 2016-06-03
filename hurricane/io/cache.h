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

#ifndef _CACHE_
#define _CACHE_

#include "../../utils/memory_utils.h"

class cache {
    unsigned char *blocks;
    unsigned long *metadata;
    unsigned long freelist;
    unsigned long *file_head;
    unsigned long *file_tail;
    unsigned long *file_ptr;
    unsigned long *file_offset;
    bool *fd_free;

public:
    cache(unsigned long block_cnt) {
      blocks = (unsigned char *) map_anon_memory(block_cnt * 4194304, true, "cache blocks", false);
      metadata = (unsigned long *) map_anon_memory(block_cnt * sizeof(unsigned long),
                                                   true,
                                                   "block metadata", false);
      freelist = 0;
      for (unsigned long i = 0; i < (block_cnt - 1); i++) {
        metadata[i] = i + 1;
      }
      metadata[block_cnt - 1] = ULONG_MAX;
      file_head = new unsigned long[2048];
      file_tail = new unsigned long[2048];
      file_ptr = new unsigned long[2048];
      file_offset = new unsigned long[2048];
      fd_free = new bool[2048];
      for (unsigned long i = 0; i < 2048; i++) {
        fd_free[i] = true;
      }
    }

    int open() {
      for (int i = 0; i < 2048; i++) {
        if (fd_free[i]) {
          fd_free[i] = false;
          file_ptr[i] = file_head[i] = file_tail[i] = ULONG_MAX;
          return i;
        }
      }
      return -1;
    }

    void read(int fd, unsigned char *buffer, unsigned long size) {
      if (file_ptr[fd] == ULONG_MAX) {
        return;
      }
      while (size) {
        unsigned long bytes_remaining = 4194304 - file_offset[fd];
        if (bytes_remaining == 0) {
          file_ptr[fd] = metadata[file_ptr[fd]];
          file_offset[fd] = 0;
        }
        bytes_remaining = 4194304 - file_offset[fd];
        unsigned long tocopy = size > bytes_remaining ? bytes_remaining : size;
        memcpy(buffer, blocks + file_ptr[fd] * 4194304 + file_offset[fd], tocopy);
        file_offset[fd] += tocopy;
        buffer += tocopy;
        size -= tocopy;
      }
    }

    void read_and_delete(int fd, unsigned char *buffer, unsigned long size) {
      if (file_ptr[fd] == ULONG_MAX) {
        return;
      }
      while (size) {
        unsigned long bytes_remaining = 4194304 - file_offset[fd];
        if (bytes_remaining == 0) {
          unsigned long freeblock = file_ptr[fd];
          file_ptr[fd] = metadata[file_ptr[fd]];
          file_offset[fd] = 0;
          file_head[fd] = file_ptr[fd];
          metadata[freeblock] = freelist;
          freelist = freeblock;
        }
        bytes_remaining = 4194304 - file_offset[fd];
        unsigned long tocopy = size > bytes_remaining ? bytes_remaining : size;
        memcpy(buffer, blocks + file_ptr[fd] * 4194304 + file_offset[fd], tocopy);
        file_offset[fd] += tocopy;
        buffer += tocopy;
        size -= tocopy;
      }
    }

    void write(int fd, unsigned char *buffer, unsigned long size) {
      if (file_ptr[fd] == ULONG_MAX) {
        if (freelist == ULONG_MAX) {
          BOOST_LOG_TRIVIAL(fatal) << "Cache full !";
          exit(-1);
        }

        file_head[fd] = file_tail[fd] = file_ptr[fd] = freelist;
        file_offset[fd] = 0;
        freelist = metadata[freelist];
        metadata[file_ptr[fd]] = ULONG_MAX;
      }
      while (size) {
        unsigned long bytes_remaining = 4194304 - file_offset[fd];
        if (bytes_remaining == 0) {
          if (freelist == ULONG_MAX) {
            BOOST_LOG_TRIVIAL(fatal) << "Cache full !";
            exit(-1);
          }
          metadata[file_ptr[fd]] = freelist;
          freelist = metadata[freelist];
          file_tail[fd] = file_ptr[fd] = metadata[file_ptr[fd]];
          file_offset[fd] = 0;
        }
        bytes_remaining = 4194304 - file_offset[fd];
        unsigned long tocopy = size > bytes_remaining ? bytes_remaining : size;
        memcpy(blocks + file_ptr[fd] * 4194304 + file_offset[fd], buffer, tocopy);
        file_offset[fd] += tocopy;
        buffer += tocopy;
        size -= tocopy;
      }
    }

    void rewind(int fd) {
      file_ptr[fd] = file_head[fd];
      file_offset[fd] = 0;
    }

    void reset(int fd) {
      if (file_head[fd] == ULONG_MAX)
        return;
      metadata[file_tail[fd]] = freelist;
      freelist = file_head[fd];
      file_head[fd] = file_tail[fd] = file_ptr[fd] = ULONG_MAX;
    }

};


#endif