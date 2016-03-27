#ifndef _CLIENT_BARRIER_
#define _CLIENT_BARRIER_

namespace slipstore {
	extern boost::asio::io_service ioService;

	class barrier_work {
  public:
      virtual void operator()() = 0;
  };

  class client_barrier {
      // Barrier sockets
      void *socket_barrier_in;
      void *socket_barrier_out;
      void *socket_local_store;

      unsigned long slipchunk;
      unsigned long machines;
      unsigned long me;

      void setup_conn() {
      	// TODO
        //////////////////////////////////// Barrier sockets
        //socket_barrier_out = zmq_socket(zmq_context, ZMQ_PUSH);
        //unsigned long barrier_ring_next = (me + 1) % machines;
        //slipstore_connect_endpoint(socket_barrier_out,
        //                           conn_client_barrier_endpoint(barrier_ring_next).c_str());
        //socket_barrier_in = zmq_socket(zmq_context, ZMQ_PULL);
        //slipstore_bind_endpoint(socket_barrier_in,
        //                        conn_client_barrier_bindpoint(me).c_str());
        /////////////////////////////////// Connection to local store
        //socket_local_store = zmq_socket(zmq_context, ZMQ_REQ);
        //slipstore_mark_low_latency(socket_local_store);
        //slipstore_connect_endpoint(socket_local_store,
        //                           conn_server_control_endpoint(me).c_str());
      }

      void flush_local_store() {
      	// TODO
        //slipstore_req_t req;
        //req.cmd = CMD_NOP;
        //// Issue request
        //(void) do_zmq_send(socket_local_store,
        //                   (unsigned char *) &req,
        //                   sizeof(slipstore_req_t),
        //                   "client barrier cmd out");
        //// Get reply
        //unsigned long rep;
        //(void) do_zmq_recv(socket_local_store,
        //                   (unsigned char *) &rep,
        //                   sizeof(unsigned long),
        //                   "client barrier rep in");
      }

  public:

      // In progress
      volatile bool in_progress;

      void teardown_conn() {
        zmq_close(socket_barrier_out);
        zmq_close(socket_barrier_in);
        zmq_close(socket_local_store);
      }

      client_barrier() {
        slipchunk = vm["slipchunk"].as < unsigned
        long > ();
        machines = pt_slipstore.get < unsigned
        long > ("machines.count");
        me = pt_slipstore.get < unsigned
        long > ("machines.me");
        setup_conn();
        in_progress = false;
        __sync_synchronize();
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

      template<typename A>
      friend
      void sync_barrier_internal(client_barrier *sl_client, A *sync_obj);

      template<typename A>
      friend
      void sync_barrier(client_barrier *sl_client, A *sync_obj);

      void work_barrier(barrier_work *work_object) {
        //unsigned long token = 0xf141516182235UL;
        //// Twice around the ring
        //if (get_me() == 0) {
        //  (void) do_zmq_send(socket_barrier_out,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier out");
        //  (void) do_zmq_recv(socket_barrier_in,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier in");
        //  // Everyone is here
        //  // Flush the local store
        //  flush_local_store();
        //  // Do the work
        //  (*work_object)();
        //  (void) do_zmq_send(socket_barrier_out,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier out");
        //  (void) do_zmq_recv(socket_barrier_in,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier in");
        //  // Extra round to ensure that everyone has done the work
        //  (void) do_zmq_send(socket_barrier_out,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier out");
        //  (void) do_zmq_recv(socket_barrier_in,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier in");
        //}
        //else {
        //  (void) do_zmq_recv(socket_barrier_in,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier in");
        //  (void) do_zmq_send(socket_barrier_out,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier out");
        //  (void) do_zmq_recv(socket_barrier_in,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier in");
        //  // Everyone is here
        //  // Flush the local store
        //  flush_local_store();
        //  // Do the work
        //  (*work_object)();
        //  (void) do_zmq_send(socket_barrier_out,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier out");
        //  // Extra round to ensure that everyone has done the work
        //  (void) do_zmq_recv(socket_barrier_in,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier in");
        //  (void) do_zmq_send(socket_barrier_out,
        //                     (unsigned char *) &token,
        //                     sizeof(unsigned long),
        //                     "barrier out");
        //}
        //// Everyone has done the work
        //__sync_synchronize();
        //in_progress = false;
      }
  };

  template<typename A>
  static void sync_barrier_internal(client_barrier *sl_client,
                                    A *sync_object) {
    if (sl_client->me == 0) {
      sync_object->db_generate();
      (void) do_zmq_send(sl_client->socket_barrier_out,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data out");
      (void) do_zmq_recv(sl_client->socket_barrier_in,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data in");
      sync_object->db_absorb();
      (void) do_zmq_send(sl_client->socket_barrier_out,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data out");
      (void) do_zmq_recv(sl_client->socket_barrier_in,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data in");
    }
    else {
      (void) do_zmq_recv(sl_client->socket_barrier_in,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data in");
      sync_object->db_merge();
      (void) do_zmq_send(sl_client->socket_barrier_out,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data out");
      (void) do_zmq_recv(sl_client->socket_barrier_in,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data in");
      sync_object->db_absorb();
      (void) do_zmq_send(sl_client->socket_barrier_out,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data out");
    }
    __sync_synchronize();
    sl_client->in_progress = false;
  }

  template<typename A>
  static void sync_barrier(client_barrier *sl_client,
                           A *sync_object) {
    sl_client->in_progress = true;
    ioService.post
        (boost::bind(&sync_barrier_internal<A>,
                     sl_client,
                     sync_object));
    while (sl_client->in_progress);
  }
}
#endif