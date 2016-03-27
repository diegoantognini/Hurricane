#ifndef _CLIENT_DRAIN_
#define _CLIENT_DRAIN_

namespace slipstore {
	struct substream_tracker {
      // Buffer and index
      unsigned char *buffer;
      unsigned long index_entries;
      unsigned long skip_index;
      unsigned long cpus;
      unsigned long **index;
      unsigned long *per_cpu_end;
      unsigned long tiles;
      // Iterator state
      unsigned long next_index;
      unsigned long next_cpu;

      void init() {
        next_index = 0;
        next_cpu = 0;
      }

      void next_io(unsigned long stream,
                   slipstore_req_t *req,
                   unsigned char *iobuf,
                   unsigned long bufsize) {
        req->size = 0;
        while (next_index < index_entries) {
          if (next_index == skip_index) {
            next_index++;
            continue;
          }
          while (next_cpu < cpus) {
            unsigned long bytes_available;
            if (bufsize == 0) {
              return;
            }
            if (next_index < (index_entries - 1)) {
              bytes_available = index[next_cpu][next_index + 1] -
                                index[next_cpu][next_index];
            }
            else {
              bytes_available = per_cpu_end[next_cpu] -
                                index[next_cpu][next_index];
            }
            if (bytes_available == 0) {
              next_cpu++;
            }
            else {
              if (bytes_available > bufsize) {
                bytes_available = bufsize;
              }
              /* have some data for i/o */
              req->size += bytes_available;
              memcpy(iobuf,
                     buffer + index[next_cpu][next_index],
                     bytes_available);
              iobuf += bytes_available;
              bufsize -= bytes_available;
              req->cmd = slipstore::CMD_DRAIN;
              req->stream = stream;
              req->partition = next_index / tiles;
              req->tile = next_index % tiles;
              index[next_cpu][next_index] += bytes_available;
            }
          }
          next_cpu = 0;
          next_index++;
          if (req->size > 0) {
            return; // Limit req to one sp
          }
        }
      }
  };

	class client_drain {
	      // Command sockets
	      void **sockets_cmd_out;
	      // Data sockets
	      void **sockets_data_out;

	      unsigned long slipchunk;
	      unsigned long machines;
	      unsigned long me;
	      cyclic *request_cycle;
	      random_selector *server_selector;
	      unsigned long batch_size;
	      unsigned long *window_mc;
	      unsigned long *size_mc;
	      unsigned char **buf_mc;
	      slipstore_req_t *req_mc;
	      bool *retry_mc;
	      rtc_clock write_time;
	      unsigned long bytes_written;

	      void setup_conn() {
	      	// TODO
	        ///////////////////////////////////// Command sockets
	        //sockets_cmd_out = new void *[machines];
	        //for (unsigned long i = 0; i < machines; i++) {
	        //  sockets_cmd_out[i] = zmq_socket(zmq_context, ZMQ_REQ);
	        //  slipstore_mark_low_latency(sockets_cmd_out[i]);
	        //  slipstore_connect_endpoint(sockets_cmd_out[i],
	        //                             conn_server_control_endpoint(i).c_str());
	        //}
	        //////////////////////////////////// Data sockets
	        //sockets_data_out = new void *[machines];
	        //for (unsigned long i = 0; i < machines; i++) {
	        //  sockets_data_out[i] = zmq_socket(zmq_context_data_out, ZMQ_PUSH);
	        //  slipstore_connect_endpoint(sockets_data_out[i],
	        //                             conn_server_data_endpoint(i, me).c_str());
	        //}
	      }

	  public:

	      void teardown_conn() {
	        for (unsigned long i = 0; i < machines; i++) {
	          zmq_close(sockets_cmd_out[i]);
	          zmq_close(sockets_data_out[i]);
	        }
	      }

	      client_drain() {
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
	        }
	        else {
	          request_cycle =
	              new cyclic(vm["slipstore_servers"].as < unsigned
	          long > (), me);
	        }
	        server_selector = new random_selector(machines, me + 256);
	        batch_size = vm["batch_size"].as < unsigned
	        long > ();
	        window_mc = new unsigned long[batch_size];
	        if (vm.count("gather_io_drain") > 0) {
	          buf_mc = new unsigned char *[batch_size];
	          for (unsigned long i = 0; i < batch_size; i++) {
	            buf_mc[i] = new unsigned char[slipchunk];
	          }
	          req_mc = new slipstore_req_t[batch_size];
	        }
	        size_mc = new unsigned long[batch_size];
	        retry_mc = new bool[batch_size];
	        bytes_written = 0;
	        for (unsigned long i = 0; i < batch_size; i++) {
	          window_mc[i] = ULONG_MAX;
	          retry_mc[i] = false;
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

	      // Client: execute a data transfer command
	      bool access_store(slipstore_req_t *req,
	                        unsigned char *buffer,
	                        unsigned long mc = ULONG_MAX) {
	      	// TODO
	      	return false;
	        //unsigned long trials;
	        //if (mc != ULONG_MAX) {
	        //  trials = machines - 1;
	        //}
	        //else {
	        //  trials = 0; // try all machines
	        //  mc = request_cycle->cyclic_next();
	        //}
	        //req->source_mc = me;
	        //write_time.start();
	        //have_outstanding_request->start();
	        //do {
	        //  // Issue request
	        //  (void) do_zmq_send(sockets_cmd_out[mc],
	        //                     (unsigned char *) req,
	        //                     sizeof(slipstore_req_t),
	        //                     "client cmd out");
	        //  // Get reply
	        //  unsigned long rep;
	        //  (void) do_zmq_recv(sockets_cmd_out[mc],
	        //                     (unsigned char *) &rep,
	        //                     sizeof(unsigned long),
	        //                     "client rep in");
	        //  // If this was a terminate or nop command we are done
	        //  if (req->cmd == CMD_TERMINATE ||
	        //      req->cmd == CMD_NOP ||
	        //      req->cmd == CMD_MEMREAD) {
	        //    write_time.stop();
	        //    have_outstanding_request->stop();
	        //    return true;
	        //  }
	        //  BOOST_ASSERT_MSG(req->cmd == CMD_DRAIN,
	        //                   "Non drain cmd to drain client");
	        //  if (rep == RESP_ACK) {
	        //    // Great, this machines agrees to serve
	        //    bytes_written += req->size;
	        //    (void) do_zmq_send(sockets_data_out[mc],
	        //                       buffer,
	        //                       req->size,
	        //                       "client data out");
	        //    write_time.stop();
	        //    have_outstanding_request->stop();
	        //    return true;
	        //  }
	        //  mc = request_cycle->cyclic_next();
	        //} while ((++trials) < request_cycle->cycle_size());
	        //write_time.stop();
	        //have_outstanding_request->stop();
	        //return false;
	      }

	      // Client: execute a data transfer command
	      unsigned long batch_access_store(slipstore_req_t *req,
	                                       unsigned char *buffer,
	                                       unsigned long chunksize,
	                                       unsigned long bufsize) {
	      	// TODO
	      	return 0;
	        //unsigned long offset = 0;
	        //unsigned long outstanding = 0;
	        //slipstore_req_t saved_req;
	        //req->source_mc = me;
	        //memcpy(&saved_req, req, sizeof(slipstore_req_t));
	        //bytes_written += bufsize;
	        //write_time.start();
	        //have_outstanding_request->start();
	        //bool do_polling = (vm.count("polling_client") > 0);
	        //do {
	        //  if (outstanding == batch_size) {
	        //    if (!do_polling) {
	        //      // Poll here
	        //      zmq_pollitem_t items[batch_size];
	        //      for (unsigned long i = 0; i < batch_size; i++) {
	        //        items[i].socket = sockets_cmd_out[window_mc[i]];
	        //        items[i].events = ZMQ_POLLIN;
	        //      }
	        //      (void) zmq_poll(items, batch_size, -1);
	        //    }
	        //  }
	        //  // Can do some work
	        //  for (unsigned long i = 0; i < batch_size; i++) {
	        //    //Complete request
	        //    if (window_mc[i] != ULONG_MAX) {
	        //      unsigned long rep;
	        //      long rc = do_zmq_recv_noblock(sockets_cmd_out[window_mc[i]],
	        //                                    (unsigned char *) &rep,
	        //                                    sizeof(unsigned long),
	        //                                    "client rep in");
	        //      if (rc >= 0) {
	        //        if (rep == RESP_ACK) {
	        //          // Complete drain
	        //          (void) do_zmq_send(sockets_data_out[window_mc[i]],
	        //                             buffer + offset,
	        //                             size_mc[i],
	        //                             "client data out");
	        //          server_selector->deselect(window_mc[i]);
	        //          offset += size_mc[i];
	        //        }
	        //        else {
	        //          bufsize += size_mc[i];
	        //        }
	        //        outstanding--;
	        //        window_mc[i] = ULONG_MAX;
	        //      }
	        //    }
	        //    //Make request
	        //    if (window_mc[i] == ULONG_MAX && bufsize > 0) {
	        //      // Issue request
	        //      window_mc[i] = server_selector->select();
	        //      if (window_mc[i] != ULONG_MAX) {
	        //        memcpy(req, &saved_req, sizeof(slipstore_req_t));
	        //        if (chunksize > bufsize) {
	        //          req->size = bufsize;
	        //        }
	        //        else {
	        //          req->size = chunksize;
	        //        }
	        //        size_mc[i] = req->size;
	        //        bufsize -= size_mc[i];
	        //        (void) do_zmq_send(sockets_cmd_out[window_mc[i]],
	        //                           (unsigned char *) req,
	        //                           sizeof(slipstore_req_t),
	        //                           "client cmd out");
	        //        outstanding++;
	        //      }
	        //    }
	        //  }
	        //} while (outstanding > 0);
	        //server_selector->deselect_all();
	        //write_time.stop();
	        //have_outstanding_request->stop();
	        //bytes_written -= bufsize;
	        //return bufsize;
	      }

	      // Client: execute a data transfer command
	      bool batch_access_store_gather_io(substream_tracker *subtracker,
	                                        unsigned long stream,
	                                        unsigned long chunksize) {
	      	// TODO
	      	return false;
	        //unsigned long outstanding = 0;
	        //unsigned long retry_outstanding = 0;
	        //write_time.start();
	        //have_outstanding_request->start();
	        //bool do_polling = (vm.count("polling_client") > 0);
	        //do {
	        //  if (outstanding == batch_size) {
	        //    if (!do_polling) {
	        //      // Poll here
	        //      zmq_pollitem_t items[batch_size];
	        //      for (unsigned long i = 0; i < batch_size; i++) {
	        //        items[i].socket = sockets_cmd_out[window_mc[i]];
	        //        items[i].events = ZMQ_POLLIN;
	        //      }
	        //      (void) zmq_poll(items, batch_size, -1);
	        //    }
	        //  }
	        //  // Can do some work
	        //  for (unsigned long i = 0; i < batch_size; i++) {
	        //    //Complete request
	        //    if (window_mc[i] != ULONG_MAX) {
	        //      unsigned long rep;
	        //      long rc = do_zmq_recv_noblock(sockets_cmd_out[window_mc[i]],
	        //                                    (unsigned char *) &rep,
	        //                                    sizeof(unsigned long),
	        //                                    "client rep in");
	        //      if (rc >= 0) {
	        //        if (rep == RESP_ACK) {
	        //          // Complete drain
	        //          (void) do_zmq_send(sockets_data_out[window_mc[i]],
	        //                             buf_mc[i],
	        //                             req_mc[i].size,
	        //                             "client data out");
	        //          server_selector->deselect(window_mc[i]);
	        //          window_mc[i] = ULONG_MAX;
	        //          outstanding--;
	        //        }
	        //        else {
	        //          window_mc[i] = ULONG_MAX;
	        //          retry_mc[i] = true;
	        //          retry_outstanding++;
	        //        }
	        //      }
	        //    }
	        //    //Make request
	        //    if (window_mc[i] == ULONG_MAX && server_selector->have_options()) {
	        //      // Issue request
	        //      if (!retry_mc[i]) {
	        //        subtracker->next_io(stream, &req_mc[i], buf_mc[i], chunksize);
	        //      }
	        //      if (req_mc[i].size != 0) {
	        //        window_mc[i] = server_selector->select();
	        //        req_mc[i].source_mc = me;
	        //        (void) do_zmq_send(sockets_cmd_out[window_mc[i]],
	        //                           (unsigned char *) &req_mc[i],
	        //                           sizeof(slipstore_req_t),
	        //                           "client cmd out");
	        //        if (!retry_mc[i]) {
	        //          outstanding++;
	        //        }
	        //        else {
	        //          retry_mc[i] = false;
	        //          retry_outstanding--;
	        //        }
	        //      }
	        //    }
	        //    if (retry_outstanding == outstanding &&
	        //        retry_outstanding != 0 &&
	        //        !server_selector->have_options()) {
	        //      // Out of space !!!
	        //      return false;
	        //    }
	        //  }
	        //} while (outstanding > 0);
	        //server_selector->deselect_all();
	        //write_time.stop();
	        //have_outstanding_request->stop();
	        //return true;
	      }


	      unsigned long get_bytes_written() {
	        write_time.print("SLIPSTORE::CLIENT::WRITE_TIME ");
	        return bytes_written;
	      }

	  };
}
#endif