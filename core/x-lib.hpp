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

#ifndef _X_LIB_
#define _X_LIB_

#include "../slipstore/memory_buffer.hpp"
#include "autotuner.hpp"
#include "../utils/clock_utils.h"
#include "../utils/memory_utils.h"
#include "../utils/barrier.h"
#include<boost/thread.hpp>
#include "../utils/barrier.h"
#include "../utils/per_cpu_data.hpp"
#include<sys/resource.h>

#define OUTBUF_SIZE 8192
namespace x_lib {

  struct work_base {
      virtual void operator()
          (unsigned long processor_id,
           configuration *config,
           x_barrier *sync,
           algorithm::per_processor_data *cpu_state,
           unsigned char *outbuf) = 0;
  };

  class x_thread {
  public:
      static x_barrier *sync;
      struct configuration *config;
      algorithm::per_processor_data *const cpu_state;
      const unsigned long processor_id;
      static volatile bool terminate;
      static struct work_base *volatile work_to_do;
      unsigned char *outbuf;

      x_thread(struct configuration *config_in,
               unsigned long processor_id_in,
               algorithm::per_processor_data *cpu_state_in)
          : config(config_in),
            cpu_state(cpu_state_in),
            processor_id(processor_id_in) {
        if (sync == NULL) { // First object
          sync = new x_barrier(config->processors);
        }
        outbuf = (unsigned char *) map_anon_memory(OUTBUF_SIZE, true, "thread outbuf");
      }

      void operator()() {
        do {
          sync->wait();
          if (terminate) {
            break;
          }
          else {
            (*work_to_do)(processor_id, config, sync, cpu_state, outbuf);
            sync->wait(); // Must synchronize before p0 exits (object is on stack)
          }
        } while (processor_id != 0);
      }

      friend class stream_IO;
  } __attribute__((__aligned__(64)));

  class null_barrier_work : public slipstore::barrier_work {
  public:
      virtual void operator()() {
      }
  };

}
#endif
