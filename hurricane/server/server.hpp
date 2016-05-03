#ifndef _SERVER_
#define _SERVER_

#include "../interface.hpp"
#include "../io/io.hpp"
#include <string>
#include <unordered_map>
#include <vector>

using std::unordered_map;
using std::string;

namespace hurricane {
	extern void *zmq_context;

	class server {
  public:
    void add_new_machine(int i = -1) {
      if(i == -1) {
        i = machines;
        ++machines;
      }

      void* socket_data_in = zmq_socket(zmq_context_data_in, ZMQ_PULL);
      hurricane_bind_endpoint(socket_data_in, conn_server_data_bindpoint(me, i).c_str());

      //void* socket_data_out  = zmq_socket(zmq_context_data_out, ZMQ_REP);
      void* socket_data_out = zmq_socket(zmq_context_data_out, ZMQ_PUSH);
      hurricane_connect_endpoint(socket_data_out, conn_client_data_endpoint(i, me).c_str());

      sockets_data_in.push_back(socket_data_in);
      sockets_data_out.push_back(socket_data_out);
    }

  protected:
      unsigned long machines;
      unsigned long me;
      // Command sockets
      void *socket_cmd_in;
      // Data sockets
      std::vector<void *> sockets_data_out;
      std::vector<void *> sockets_data_in;

      // Hurricane chunk size
      unsigned long slipchunk;
      // Handle for IO code
      unordered_map<string, hurricane::io*>* disks;

      void setup_conn() {
        /////////////////////////////////// Command sockets
        socket_cmd_in = zmq_socket(zmq_context, ZMQ_REP);
        hurricane_mark_low_latency(socket_cmd_in);
        hurricane_bind_endpoint(socket_cmd_in, conn_server_control_bindpoint(me).c_str());
        ////////////////////////////////// Data sockets
        for (unsigned long i = 0; i < machines; i++)
          add_new_machine(i);
      }

      void teardown_conn() {
        zmq_close(socket_cmd_in);
        for (unsigned long i = 0; i < machines; i++) {
          zmq_close(sockets_data_in[i]);
          zmq_close(sockets_data_out[i]);
        }
      }

  public:
      server(unordered_map<string, hurricane::io*>* disks_in) {
        machines = pt_hurricane.get < unsigned long > ("machines.count");
        me = pt_hurricane.get < unsigned long > ("machines.me");
        slipchunk = vm["slipchunk"].as < unsigned long > ();
        disks = disks_in;
      }


      // Server
      virtual void operator()() {
        BOOST_LOG_TRIVIAL(fatal) << "Called operator () on base server class!";
        exit(-1);
      }
  };
}
#endif