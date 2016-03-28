#ifndef _HELP_SKIP_
#define _HELP_SKIP_

#include "help_base.hpp"
#include "help_heuristic.hpp"
#include "../../utils/cyclic.hpp"

namespace slipstore {
	class help_skip : public help_base {
	      // Track helping
	      bool **helped_me;
	      bool *closed;
	      cyclic *helper_cycle;
	      unsigned long machines;
	      unsigned long super_partitions;
	      unsigned long me;
	      help_heuristics heuristics;
	      volatile unsigned long lock;
	      volatile int *semaphores;

	      unsigned long count_helpers(unsigned long superp) {
	        unsigned long cnt = 0;
	        for (unsigned long i = 0; i < machines; i++) {
	          cnt += (helped_me[superp][i] ? 1 : 0);
	        }
	        return cnt;
	      }

	      void lock_state() {
	        do {
	          while (lock == 1);
	        } while (!__sync_bool_compare_and_swap(&lock, 0, 1));
	      }

	      void unlock_state() {
	        __sync_synchronize();
	        lock = 0;
	      }

	  public:
	      help_skip(unsigned long tile_size_in,
	                unsigned long machines_in,
	                unsigned long me_in,
	                io *disks_in,
	                unsigned long partitions_in)
	          : machines(machines_in),
	            super_partitions(0),
	            me(me_in),
	            heuristics(tile_size_in, machines_in, disks_in, partitions_in) {
	        lock = 0;
	        semaphores = new int[machines];
	        for (unsigned long i = 0; i < machines; i++) {
	          semaphores[i] = 0;
	        }
	        helped_me = NULL;
	        closed = NULL;
	        helper_cycle = new cyclic(machines, me);
	      }

	      virtual void setup(unsigned long super_partitions_in) {
	        super_partitions = super_partitions_in;
	        helped_me = new bool *[super_partitions];
	        closed = new bool[super_partitions];
	        for (unsigned long i = 0; i < super_partitions; i++) {
	          helped_me[i] = new bool[machines];
	          closed[i] = false;
	          for (unsigned long j = 0; j < machines; j++) {
	            helped_me[i][j] = false;
	          }
	        }
	      }

	      // Help tracking stuff
	      virtual void reset() {
	        for (unsigned long i = 0; i < super_partitions; i++) {
	          closed[i] = false;
	          for (unsigned long j = 0; j < machines; j++) {
	            helped_me[i][j] = false;
	          }
	        }
	        __sync_synchronize();
	      }

	      virtual unsigned long next_helper(unsigned long superp) {
	        for (unsigned long i = 0; i < machines; i++) {
	          unsigned long mc = helper_cycle->cyclic_next();
	          if (mc == me) { continue; }
	          if (helped_me[superp][mc]) {
	            helped_me[superp][mc] = false;
	            return mc;
	          }
	        }
	        return ULONG_MAX;
	      }

	      virtual unsigned long make_decision(slipstore_req_t *req) {
	        unsigned long resp;
	        lock_state();
	        if (!closed[req->partition]) {
	          resp = heuristics.think(req, count_helpers(req->partition) + 1); // workers includes me
	        }
	        else {
	          heuristics.help_offers++;
	          resp = RESP_NACK;
	        }
	        if (resp == RESP_ACK) {
	          helped_me[req->partition][req->source_mc] = true;
	        }
	        unlock_state();
	        return resp;
	      }

	      virtual void dump_stats() {
	        heuristics.dump_stats();
	      }

	      virtual void close(unsigned long superp) {
	        lock_state();
	        closed[superp] = true;
	        unlock_state();
	      }

	      virtual void sem_down(unsigned long mc) {
	        (void) __sync_fetch_and_sub(&semaphores[mc], 1);
	        while (semaphores[mc] < 0);
	      }

	      virtual void sem_up(unsigned long mc) {
	        (void) __sync_fetch_and_add(&semaphores[mc], 1);
	      }

	      virtual void init_order(unsigned long stream) {
	        heuristics.init_order(stream);
	      }

	      virtual unsigned long order_next(unsigned long index) {
	        return heuristics.order_next(index);
	      }

	      virtual ~help_skip() {
	        delete (closed);
	        delete (semaphores);
	        for (unsigned long i = 0; i < super_partitions; i++) {
	          delete (helped_me[i]);
	        }
	        delete (helped_me);
	      }
	  };
}
#endif