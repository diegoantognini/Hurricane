#ifndef _HURRICANE_UTILS_
#define _HURRICANE_UTILS_

#include <zmq.h>

namespace hurricane {
	  static
  void hurricane_bind_endpoint(void *socket, const char *endpoint) {
    int rc = zmq_bind(socket, endpoint);
    if (rc != 0) {
      BOOST_LOG_TRIVIAL(fatal) <<
      "HURRICANE::COMM Unable to setup listening socket at "
      << endpoint;
      perror("zmq_bind:");
      exit(-1);
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "HURRICANE::COMM Listening at "
      << endpoint;
    }
  }

  static
  void hurricane_connect_endpoint(void *socket, const char *endpoint) {
    BOOST_LOG_TRIVIAL(info) << "HURRICANE::COMM Connecting to " <<
    endpoint;
    zmq_connect(socket, endpoint);
  }

  static
  void hurricane_mark_low_latency(void *socket) {
#ifdef LOW_LATENCY_CONTROL
    int tos_value = IPTOS_LOWDELAY;
    int ret = zmq_setsockopt(socket, ZMQ_TOS, &tos_value, sizeof(tos_value));
    if(ret == -1) {
      BOOST_LOG_TRIVIAL(warn) 
  << "HURRICANE: Unable to make socket low latency: "
  << strerror(errno);
    }
#endif
  }

}
#endif