#ifndef _HELP_HEURISTIC_
#define _HELP_HEURISTIC_

#include "../io/io.hpp"

namespace slipstore {
	class help_heuristics {
	      bool policy_help_all;
	      bool policy_help_none;
	      bool policy_heuristic_new;
	      double alpha;
	      bool sorted_order;
	      unsigned long tile_size;
	      unsigned long machines;
	      io *disks;
	      unsigned long partitions;
	      unsigned long *partition_order;
	  public:
	      unsigned long help_offers;
	      unsigned long help_offers_accepted;

	      help_heuristics(unsigned long tile_size_in,
	                      unsigned long machines_in,
	                      io *disks_in,
	                      unsigned long partitions_in)
	          : tile_size(tile_size_in),
	            machines(machines_in),
	            disks(disks_in),
	            partitions(partitions_in) {
	        policy_help_all = (vm.count("policy_help_all") > 0);
	        policy_help_none = (vm.count("policy_help_none") > 0);
	        policy_heuristic_new = (vm.count("policy_heuristic_new") > 0);
	        alpha = atof(vm["alpha"].as<std::string>().c_str());
	        sorted_order = (vm.count("sorted_order") > 0);
	        if (policy_help_all) {
	          BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_POLICY ALL";
	        }
	        else if (policy_help_none) {
	          BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_POLICY NONE";
	        }
	        else if (policy_heuristic_new) {
	          BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_POLICY HEURISTIC_NEW";
	        }
	        else {
	          BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_POLICY HEURISTIC";
	        }
	        partition_order = new unsigned long[partitions_in];
	        help_offers = 0;
	        help_offers_accepted = 0;
	      }

	      unsigned long think(slipstore_req_t *req,
	                          unsigned long current_workers) {
	        bool offer_help_sync = (req->size == 1);
	        help_offers++;
	        unsigned long resp;
	        if (policy_help_all) {
	          resp = RESP_ACK;
	        }
	        else if (policy_help_none) {
	          resp = RESP_NACK;
	        }
	        else if (policy_heuristic_new) {
	          unsigned long tile_sync_cost =
	              offer_help_sync ? tile_size : 0;
	          unsigned long bytes_left = machines *
	                                     (double) disks->get_bytes_left(req->stream,
	                                                                    req->partition,
	                                                                    req->tile);
	          double load_estimate = ((double) bytes_left) / current_workers;
	          // New load estimate
	          double new_load_estimate =
	              // Copy and help
	              (bytes_left + tile_size) /
	              (1.0 + (double) current_workers) +
	              // Sync serially
	              ((double) tile_sync_cost);
	          if (new_load_estimate < alpha * load_estimate) {
	            resp = RESP_ACK;
	          }
	          else {
	            resp = RESP_NACK;
	          }
	        }
	        else {
	          unsigned long tile_sync_cost =
	              offer_help_sync ? tile_size : 0;
	          double load_estimate = machines *
	                                 (double) disks->get_bytes_left(req->stream,
	                                                                req->partition,
	                                                                req->tile);
	          // New load estimate
	          double new_load_estimate =
	              (load_estimate + tile_size + tile_sync_cost) /
	              (1.0 + 1.0 / (double) current_workers);
	          if (new_load_estimate < load_estimate) {
	            resp = RESP_ACK;
	          }
	          else {
	            resp = RESP_NACK;
	          }
	        }
	        if (resp == RESP_ACK) {
	          help_offers_accepted++;
	        }
	        return resp;
	      }

	      void init_order(unsigned long stream) {
	        if (stream == ULONG_MAX || !sorted_order) {
	          for (unsigned long i = 0; i < partitions; i++) {
	            partition_order[i] = i;
	          }
	          return;
	        }
	        // Simple insertion sort
	        partition_order[0] = 0;
	        for (unsigned long i = 1; i < partitions; i++) {
	          unsigned long key = disks->get_bytes_left(stream, i, 0);
	          unsigned long j;
	          for (j = i; j != 0; j--) {
	            if (disks->get_bytes_left(stream, partition_order[j - 1], 0) < key) {
	              partition_order[j] = partition_order[j - 1];
	            }
	            else {
	              break;
	            }
	          }
	          partition_order[j] = i;
	        }
	      }

	      unsigned long order_next(unsigned long index) {
	        return partition_order[index];
	      }

	      void dump_stats() {
	        BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_OFFERS_RECVD "
	        << help_offers;
	        BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_OFFERS_ACCEPTED "
	        << help_offers_accepted;
	        double ratio;
	        if (help_offers > 0) {
	          ratio = ((double) help_offers_accepted) / help_offers;
	        }
	        else {
	          ratio = ((double) 0);
	        }
	        BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_ACCEPTANCE_RATIO "
	        << ratio;
	      }
	};
}
#endif