#ifndef _SLIPSTORE_UTILS_
#define _SLIPSTORE_UTILS_

#include <zmq.h>

namespace slipstore {
	  static
  void slipstore_bind_endpoint(void *socket, const char *endpoint) {
    int rc = zmq_bind(socket, endpoint);
    if (rc != 0) {
      BOOST_LOG_TRIVIAL(fatal) <<
      "SLIPSTORE::COMM Unable to setup listening socket at "
      << endpoint;
      perror("zmq_bind:");
      exit(-1);
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::COMM Listening at "
      << endpoint;
    }
  }

  static
  void slipstore_connect_endpoint(void *socket, const char *endpoint) {
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::COMM Connecting to " <<
    endpoint;
    zmq_connect(socket, endpoint);
  }

  static
  void slipstore_mark_low_latency(void *socket) {
#ifdef LOW_LATENCY_CONTROL
    int tos_value = IPTOS_LOWDELAY;
    int ret = zmq_setsockopt(socket, ZMQ_TOS, &tos_value, sizeof(tos_value));
    if(ret == -1) {
      BOOST_LOG_TRIVIAL(warn) 
  << "SLIPSTORE: Unable to make socket low latency: "
  << strerror(errno);
    }
#endif
  }

}
#endif