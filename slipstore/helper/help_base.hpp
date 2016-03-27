#ifndef _HELP_BASE_
#define _HELP_BASE_

#include "../io/io.hpp"

namespace slipstore {
  class help_base {
  public:
      virtual void setup(unsigned long superps) = 0;

      virtual unsigned long next_helper(unsigned long superp) = 0;

      virtual unsigned long make_decision(slipstore_req_t *req) = 0;

      virtual void dump_stats() = 0;

      virtual void reset() = 0;

      virtual void close(unsigned long superp) = 0;

      virtual void sem_down(unsigned long mc) = 0;

      virtual void sem_up(unsigned long mc) = 0;

      virtual void init_order(unsigned long stream) = 0;

      virtual unsigned long order_next(unsigned long index) = 0;

      virtual ~help_base() { }
  };
}
#endif