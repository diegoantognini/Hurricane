#ifndef _SERVER_
#define _SERVER_

#include "../interface.hpp"
#include "../io/io.hpp"
#include "../helper/help_base.hpp"
#include "../helper/help_skip.hpp"

namespace slipstore {
	extern void *zmq_context;

	class server {
  protected:
      unsigned long machines;
      unsigned long me;
      // Command sockets
      void *socket_cmd_in;
      // Data sockets
      void **sockets_data_out;
      void **sockets_data_in;
      // Slipstore chunk size
      unsigned long slipchunk;
      // Handle for IO code
      io *disks;
      // Handle for help heuristics
      help_base *help_stuff;

      void setup_conn() {
        /////////////////////////////////// Command sockets
        socket_cmd_in = zmq_socket(zmq_context, ZMQ_REP);
        // TODO
        //slipstore_mark_low_latency(socket_cmd_in);
        //slipstore_bind_endpoint(socket_cmd_in, conn_server_control_bindpoint(me).c_str());
        //////////////////////////////////// Data sockets
        //sockets_data_in = new void *[machines];
        //sockets_data_out = new void *[machines];
        //for (unsigned long i = 0; i < machines; i++) {
        //  sockets_data_in[i] = zmq_socket(zmq_context_data_in, ZMQ_PULL);
        //  slipstore_bind_endpoint(sockets_data_in[i],
        //                          conn_server_data_bindpoint(me, i).c_str());
        //  //sockets_data_out[i] = zmq_socket(zmq_context_data_out, ZMQ_REP);
        //  sockets_data_out[i] = zmq_socket(zmq_context_data_out, ZMQ_PUSH);
        //  slipstore_connect_endpoint(sockets_data_out[i],
        //                             conn_client_data_endpoint(i, me).c_str());
        //}
      }

      void teardown_conn() {
        zmq_close(socket_cmd_in);
        for (unsigned long i = 0; i < machines; i++) {
          zmq_close(sockets_data_in[i]);
          zmq_close(sockets_data_out[i]);
        }
      }

  public:
      server(io *disks_in, unsigned long tile_size_in, unsigned long partitions) {
        machines = pt_slipstore.get < unsigned
        long > ("machines.count");
        me = pt_slipstore.get < unsigned
        long > ("machines.me");
        slipchunk = vm["slipchunk"].as < unsigned
        long > ();
        disks = disks_in;
        //help_stuff = new help_original(tile_size_in, machines, me, disks, partitions);
        help_stuff = new help_skip(tile_size_in, machines, me, disks, partitions);
      }

      // Trapdoor to the help
      help_base *help_handle() {
        return help_stuff;
      }

      // Server
      virtual void operator()() {
        delete (help_stuff);
        BOOST_LOG_TRIVIAL(fatal) << "Called operator () on base server class!";
        exit(-1);
      }
  };
}
#endif