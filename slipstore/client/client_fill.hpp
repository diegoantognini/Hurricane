#ifndef _CLIENT_FILL_
#define _CLIENT_FILL_

#include "../../utils/random_selector.hpp"
#include "../../utils/cyclic.hpp"
#include "../../utils/busy_counter.hpp"
#include "../../utils/conn_utils.h"
#include "../../utils/slipstore_utils.h"
#include "../../utils/zmq_utils.h"

namespace slipstore {
	extern void *zmq_context;
	extern busy_counter *have_outstanding_request;
	extern void *zmq_context_data_in;

	class client_fill {
		      // Command sockets
	      void **sockets_cmd_out;
	      // Data sockets
	      void **sockets_data_in;

	      unsigned long slipchunk;
	      unsigned long machines;
	      unsigned long me;
	      cyclic *request_cycle;
	      cyclic *reset_cycle;
	      random_selector *server_selector;
	      unsigned long batch_size;
	      unsigned long *window_mc;
	      unsigned long *size_mc;
	      bool *ready;
	      unsigned long disk_bytes_read;
	      unsigned long mem_bytes_read;
	      rtc_clock disk_read_time;
	      rtc_clock mem_read_time;

	      void setup_conn() {
	        /////////////////////////////////// Command sockets
	        sockets_cmd_out = new void *[machines];
	        for (unsigned long i = 0; i < machines; i++) {
	          sockets_cmd_out[i] = zmq_socket(zmq_context, ZMQ_REQ);
	          slipstore_mark_low_latency(sockets_cmd_out);
	          slipstore_connect_endpoint(sockets_cmd_out[i],
	                                     conn_server_control_endpoint(i).c_str());
	        }
	        ////////////////////////////////// Data sockets
	        sockets_data_in = new void *[machines];
	        for (unsigned long i = 0; i < machines; i++) {
	          //sockets_data_in[i] = zmq_socket(zmq_context_data_in, ZMQ_REQ);
	          sockets_data_in[i] = zmq_socket(zmq_context_data_in, ZMQ_PULL);
	          slipstore_bind_endpoint(sockets_data_in[i],
	                                  conn_client_data_bindpoint(me, i).c_str());
	        }
	      }

	  public:

	      void teardown_conn() {
	        for (unsigned long i = 0; i < machines; i++) {
	          zmq_close(sockets_cmd_out[i]);
	          zmq_close(sockets_data_in[i]);
	        }
	      }

	      client_fill() {
	        slipchunk = vm["slipchunk"].as < unsigned
	        long > ();
	        machines = pt_slipstore.get < unsigned
	        long > ("machines.count");
	        me = pt_slipstore.get < unsigned
	        long > ("machines.me");
	        setup_conn();
	        if (vm["slipstore_servers"].as < unsigned
	        long > () == ULONG_MAX) {
	          request_cycle = new cyclic(machines, me);
	          reset_cycle = new cyclic(machines, me);
	        }
	        else {
	          request_cycle =
	              new cyclic(vm["slipstore_servers"].as < unsigned
	          long > (), me);
	          reset_cycle =
	              new cyclic(vm["slipstore_servers"].as < unsigned
	          long > (), me);
	        }
	        server_selector = new random_selector(machines, me + 128);
	        batch_size = vm["batch_size"].as < unsigned
	        long > ();
	        window_mc = new unsigned long[batch_size];
	        size_mc = new unsigned long[batch_size];
	        ready = new bool[batch_size];
	        disk_bytes_read = 0;
	        mem_bytes_read = 0;
	        for (unsigned long i = 0; i < batch_size; i++) {
	          window_mc[i] = ULONG_MAX;
	        }
	      }

	      unsigned long get_machines() {
	        return machines;
	      }

	      unsigned long get_me() {
	        return me;
	      }

	      unsigned long get_slipchunk() {
	        return slipchunk;
	      }

	      bool check_empty(unsigned long stream) {
	        bool empty = true;
	        slipstore_req_t req;
	        req.stream = stream;
	        req.cmd = CMD_EOF;
	        have_outstanding_request->start();
	        disk_read_time.start();
	        for (unsigned long i = 0; i < machines; i++) {
	          // Issue request
	          (void) do_zmq_send(sockets_cmd_out[i],
	                             (unsigned char *) &req,
	                             sizeof(slipstore_req_t),
	                             "client cmd out");
	          // Get reply
	          unsigned long rep;
	          (void) do_zmq_recv(sockets_cmd_out[i],
	                             (unsigned char *) &rep,
	                             sizeof(unsigned long),
	                             "client rep in");
	          empty = empty && (rep == RESP_ACK);
	        }
	        disk_read_time.stop();
	        have_outstanding_request->stop();
	        return empty;
	      }

	      void reset(unsigned long stream,
	                 unsigned long partition,
	                 unsigned long tile) {
	        slipstore_req_t req;
	        req.stream = stream;
	        req.partition = partition;
	        req.tile = tile;
	        req.cmd = CMD_RESET;
	        have_outstanding_request->start();
	        disk_read_time.start();
	        for (unsigned long i = 0; i < machines; i++) {
	          unsigned long mc = reset_cycle->cyclic_next();
	          // Issue request
	          (void) do_zmq_send(sockets_cmd_out[mc],
	                             (unsigned char *) &req,
	                             sizeof(slipstore_req_t),
	                             "client cmd out");
	          // Get reply
	          unsigned long rep;
	          (void) do_zmq_recv(sockets_cmd_out[mc],
	                             (unsigned char *) &rep,
	                             sizeof(unsigned long),
	                             "client rep in");
	        }
	        disk_read_time.stop();
	        have_outstanding_request->stop();
	      }

	      // Send a release message
	      void send_semup(unsigned long mc) {
	        slipstore_req_t req;
	        req.cmd = CMD_SEMUP;
	        req.source_mc = me;
	        (void) do_zmq_send(sockets_cmd_out[mc],
	                           (unsigned char *) &req,
	                           sizeof(slipstore_req_t),
	                           "client cmd out");
	        // Get reply
	        unsigned long rep;
	        (void) do_zmq_recv(sockets_cmd_out[mc],
	                           (unsigned char *) &rep,
	                           sizeof(unsigned long),
	                           "client rep in");
	      }

	      // Client: execute a data transfer command
	      bool access_store(slipstore_req_t *req,
	                        unsigned char *buffer,
	                        unsigned long mc = ULONG_MAX) {
	        unsigned long trials;
	        unsigned long *bytes_read;
	        rtc_clock *read_time;
	        if (req->cmd == CMD_FILL && req->stream == STREAM_MEMBUFFER) {
	          bytes_read = &mem_bytes_read;
	          read_time = &mem_read_time;
	        }
	        else {
	          bytes_read = &disk_bytes_read;
	          read_time = &disk_read_time;
	        }
	        if (mc != ULONG_MAX) {
	          trials = machines - 1;
	        }
	        else {
	          trials = 0; // try all machines
	          mc = request_cycle->cyclic_next();
	        }
	        req->source_mc = me;
	        have_outstanding_request->start();
	        read_time->start();
	        do {
	          // Issue request
	          (void) do_zmq_send(sockets_cmd_out[mc],
	                             (unsigned char *) req,
	                             sizeof(slipstore_req_t),
	                             "client cmd out");
	          // Get reply
	          unsigned long rep;
	          (void) do_zmq_recv(sockets_cmd_out[mc],
	                             (unsigned char *) &rep,
	                             sizeof(unsigned long),
	                             "client rep in");
	          // If this was a terminate or nop command we are done
	          if (req->cmd == CMD_TERMINATE || req->cmd == CMD_NOP) {
	            have_outstanding_request->stop();
	            read_time->stop();
	            return true;
	          }
	          BOOST_ASSERT_MSG(req->cmd == CMD_FILL || req->cmd == CMD_SEEK_AND_FILL,
	                           "Non fill cmd to fill client");
	          if (rep == RESP_ACK) {
	            // Great, this machines agrees to serve
	            /*
	            unsigned int dummy = 0xdeadbeef;
	            (void) do_zmq_send(sockets_data_in[mc],
	                   (unsigned char *)&dummy,
	                   sizeof(int),
	                   "dummy");
	            */
	            req->size = do_zmq_recv(sockets_data_in[mc],
	                                    buffer,
	                                    req->size,
	                                    "client data in");
	            (*bytes_read) += req->size;
	            have_outstanding_request->stop();
	            read_time->stop();
	            return true;
	          }
	          mc = request_cycle->cyclic_next();
	        } while ((++trials) < request_cycle->cycle_size());
	        read_time->stop();
	        have_outstanding_request->stop();
	        return false;
	      }

	      // Client: execute a data transfer command
	      unsigned long batch_access_store(slipstore_req_t *req,
	                                       unsigned char *buffer,
	                                       unsigned long chunksize,
	                                       unsigned long bufsize) {
	        unsigned long offset = 0;
	        unsigned long used_bytes = 0;
	        unsigned long outstanding = 0;
	        slipstore_req_t saved_req;
	        req->source_mc = me;
	        memcpy(&saved_req, req, sizeof(slipstore_req_t));
	        disk_read_time.start();
	        have_outstanding_request->start();
	        bool do_polling = (vm.count("polling_client") > 0);
	        do {
	          if (outstanding == batch_size) {
	            // Poll here
	            if (!do_polling) {
	              zmq_pollitem_t items[batch_size];
	              for (unsigned long i = 0; i < batch_size; i++) {
	                items[i].socket = sockets_cmd_out[window_mc[i]];
	                items[i].events = ZMQ_POLLIN;
	              }
	              (void) zmq_poll(items, batch_size, -1);
	            }
	          }
	          // Initiate as many transfers as possible
	          for (unsigned long i = 0; i < batch_size; i++) {
	            //Complete request
	            if (window_mc[i] != ULONG_MAX && !ready[i]) {
	              unsigned long rep;
	              long rc = do_zmq_recv_noblock(sockets_cmd_out[window_mc[i]],
	                                            (unsigned char *) &rep,
	                                            sizeof(unsigned long),
	                                            "client rep in");
	              if (rc >= 0) {
	                if (rep == RESP_ACK) {
	                  /*
	                  unsigned int dummy = 0xdeadbeef;
	                  (void) do_zmq_send(sockets_data_in[window_mc[i]],
	                         (unsigned char *)&dummy,
	                         sizeof(int),
	                         "dummy");
	                  */
	                  ready[i] = true;
	                }
	                else {
	                  used_bytes -= size_mc[i];
	                  outstanding--;
	                  window_mc[i] = ULONG_MAX;
	                }
	              }
	            }
	          }
	          // Complete transfers and issue fresh requests
	          for (unsigned long i = 0; i < batch_size; i++) {
	            if (window_mc[i] != ULONG_MAX && ready[i]) {
	              // Complete fill
	              unsigned long recvd_bytes;
	              recvd_bytes = do_zmq_recv(sockets_data_in[window_mc[i]],
	                                        buffer + offset,
	                                        size_mc[i],
	                                        "client data in");
	              server_selector->deselect(window_mc[i]);
	              offset += recvd_bytes;
	              used_bytes -= (size_mc[i] - recvd_bytes);
	              outstanding--;
	              window_mc[i] = ULONG_MAX;
	            }
	            //Make request
	            if (window_mc[i] == ULONG_MAX && used_bytes <= (bufsize - chunksize)) {
	              // Issue request
	              window_mc[i] = server_selector->select();
	              ready[i] = false;
	              if (window_mc[i] != ULONG_MAX) {
	                memcpy(req, &saved_req, sizeof(slipstore_req_t));
	                req->size = chunksize;
	                size_mc[i] = req->size;
	                used_bytes += size_mc[i];
	                (void) do_zmq_send(sockets_cmd_out[window_mc[i]],
	                                   (unsigned char *) req,
	                                   sizeof(slipstore_req_t),
	                                   "client cmd out");
	                outstanding++;
	              }
	            }
	          }
	        } while (outstanding > 0);
	        server_selector->deselect_all();
	        disk_read_time.stop();
	        have_outstanding_request->stop();
	        disk_bytes_read += used_bytes;
	        return used_bytes;
	      }

	      void print_times() {
	        disk_read_time.print("SLIPSTORE::CLIENT::DISK_READ_TIME ");
	        mem_read_time.print("SLIPSTORE::CLIENT::MEM_READ_TIME ");
	      }

	      unsigned long get_disk_bytes_read() {
	        return disk_bytes_read;
	      }

	      unsigned long get_mem_bytes_read() {
	        return mem_bytes_read;
	      }
	  };
}
#endif