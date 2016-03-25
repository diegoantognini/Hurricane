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

#ifndef _MEMORY_UTILS_
#define _MEMORY_UTILS_

#include <sys/mman.h>

extern unsigned long stat_bytes_read;
extern unsigned long stat_bytes_written;

static void memory_utils_err(const char *op, const char *subop) {
  BOOST_LOG_TRIVIAL(error) << op << "::" << subop << " failed:" <<
  strerror(errno);
}

//! Used to allocate *seriously large* amounts of 
//! zeroed out memory
static void *map_anon_memory(unsigned long size,
                             bool mlocked,
                             const char *operation,
                             bool zero = false) {
  void *space = mmap(NULL, size > 0 ? size : 4096,
                     PROT_READ | PROT_WRITE,
                     MAP_ANONYMOUS | MAP_SHARED, -1, 0);
  if (space == MAP_FAILED) {
    memory_utils_err(operation, "mmap");
    exit(-1);
  }
  if (mlocked) {
    if (mlock(space, size) < 0) {
      memory_utils_err(operation, "mlock");
    }
  }
  if (zero) {
    memset(space, 0, size);
  }
  return space;
}

static void truncate_file(int fd) {
  if (ftruncate(fd, 0)) {
    BOOST_LOG_TRIVIAL(fatal) << "file truncate failed:" << strerror(errno);
    exit(-1);
  }
}

static void truncate_file_to_size(int fd, unsigned long size) {
  if (ftruncate(fd, size)) {
    BOOST_LOG_TRIVIAL(fatal) << "file truncate failed:" << strerror(errno);
    exit(-1);
  }
}

static void check_lseek_result(off_t result) {
  if (result == (off_t) -1) {
    BOOST_LOG_TRIVIAL(fatal) << "lseek failed:" << strerror(errno);
    exit(-1);
  }
}

static unsigned long get_file_size(int fd) {
  off_t fsize, cpos;
  cpos = lseek(fd, 0, SEEK_CUR);
  check_lseek_result(cpos);
  fsize = lseek(fd, 0, SEEK_END);
  check_lseek_result(fsize);
  cpos = lseek(fd, cpos, SEEK_SET);
  check_lseek_result(cpos);
  return fsize;
}

static void rewind_file(int fd) {
  unsigned long cpos = lseek(fd, 0, SEEK_SET);
  check_lseek_result(cpos);
}

static void set_filepos(int fd, unsigned long pos) {
  unsigned long cpos = lseek(fd, pos, SEEK_SET);
  check_lseek_result(cpos);
}

static void write_to_file(int fd,
                          unsigned char *output,
                          unsigned long bytes_to_write) {
  __sync_fetch_and_add(&stat_bytes_written, bytes_to_write);
  while (bytes_to_write) {
    unsigned long bytes_written = write(fd, output, bytes_to_write);
    if (bytes_written == -1UL) {
      if (errno != EAGAIN) {
        BOOST_LOG_TRIVIAL(fatal) <<
        "Stream writeout unsuccessful:" << strerror(errno);
        exit(-1);
      }
    }
    else {
      output += bytes_written;
      bytes_to_write -= bytes_written;
    }
  }
}

static void read_from_file(int fd,
                           unsigned char *input,
                           unsigned long bytes_to_read) {
  __sync_fetch_and_add(&stat_bytes_read, bytes_to_read);
  while (bytes_to_read) {
    unsigned long bytes_read = read(fd, input, bytes_to_read);
    if (bytes_read == -1UL) {
      if (errno != EAGAIN) {
        BOOST_LOG_TRIVIAL(fatal) <<
        "Stream readin unsuccessful:" << strerror(errno);
        exit(-1);
      }
    }
    else if (bytes_read == 0) {
      // Note: silently return if EOF
      break;
    }
    else {
      input += bytes_read;
      bytes_to_read -= bytes_read;
    }
  }
}

static void unlink_file(char *fname) {
  int ret = unlink(fname);
  if (ret < 0) {
    BOOST_LOG_TRIVIAL(warn)
    << "Unable to unlink file ("
    << fname
    << "):"
    << strerror(errno);
  }
}

#endif
