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

#ifndef _HURRICANE_IO_
#define _HURRICANE_IO_
namespace hurricane {

  struct fsnap {
      unsigned long pos;
      unsigned long size;
  };

  extern unsigned long quota;
  extern unsigned long usage;
  extern unsigned long max_quota_usage;

  class hurricane_io {
  public:
      virtual unsigned long fill(unsigned char *buffer,
                                 unsigned long size) = 0;

      virtual unsigned long seek_and_fill(unsigned char *buffer,
                                          unsigned long offset,
                                          unsigned long size) = 0;

      virtual void drain(unsigned char *buffer,
                         unsigned long size) = 0;

      virtual bool eof() = 0;

      virtual unsigned long size() = 0;

      virtual unsigned long left() = 0;

      virtual void rewind() = 0;

      virtual void trunc() = 0;

      virtual ~hurricane_io() { }
  };
}
#endif
