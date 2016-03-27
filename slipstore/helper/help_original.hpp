#ifndef _HELP_ORIGINAL_
#define _HELP_ORIGINAL_

#include "help_base.hpp"

namespace slipstore {
	class help_original : public help_base {
	      // Track helping
	      bool *helped_me;
	      cyclic *helper_cycle;
	      unsigned long machines;
	      unsigned long me;
	      help_heuristics heuristics;

	      unsigned long count_helpers() {
	        unsigned long cnt = 0;
	        for (unsigned long i = 0; i < machines; i++) {
	          cnt += (helped_me[i] ? 1 : 0);
	        }
	        return cnt;
	      }

	  public:
	      help_original(unsigned long tile_size_in,
	                    unsigned long machines_in,
	                    unsigned long me_in,
	                    io *disks_in,
	                    unsigned long partitions_in)
	          : machines(machines_in),
	            me(me_in),
	            heuristics(tile_size_in, machines_in, disks_in, partitions_in) {
	        helped_me = new bool[machines];
	        helper_cycle = new cyclic(machines, me);
	      }

	      // Help tracking stuff
	      void reset() {
	        for (unsigned long i = 0; i < machines; i++) {
	          helped_me[i] = false;
	        }
	      }

	      virtual unsigned long next_helper(unsigned long next_superp) {
	        for (unsigned long i = 0; i < machines; i++) {
	          unsigned long mc = helper_cycle->cyclic_next();
	          if (mc == me) { continue; }
	          if (helped_me[mc]) {
	            helped_me[mc] = false;
	            return mc;
	          }
	        }
	        return ULONG_MAX;
	      }

	      virtual unsigned long make_decision(slipstore_req_t *req) {
	        unsigned long resp;
	        resp = heuristics.think(req, count_helpers() + 1); // workers includes me
	        if (resp == RESP_ACK) {
	          helped_me[req->source_mc] = true;
	        }
	        return resp;
	      }

	      virtual void dump_stats() {
	        heuristics.dump_stats();
	      }

	      virtual void close(unsigned long superp) {
	        BOOST_LOG_TRIVIAL(fatal) << "Close not allowed in original helper";
	      }

	      virtual void setup(unsigned long superps) {
	        BOOST_LOG_TRIVIAL(fatal) << "Setup not allowed in original helper";
	      }

	      virtual void sem_down(unsigned long mc) {
	        BOOST_LOG_TRIVIAL(fatal) << "await_release not allowed in original helper";
	      }

	      virtual void sem_up(unsigned long mc) {
	        BOOST_LOG_TRIVIAL(fatal) << "release not allowed in original helper";
	      }

	      virtual void init_order(unsigned long stream) {
	        heuristics.init_order(stream);
	      }

	      virtual unsigned long order_next(unsigned long index) {
	        return heuristics.order_next(index);
	      }

	      virtual ~help_original() {
	      }
	  };
}
#endif