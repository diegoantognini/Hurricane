#ifndef _ZMQ_UTILS_
#define _ZMQ_UTILS_

#include <zmq.h>
#include <netinet/ip.h>

namespace slipstore {
	  static
    unsigned long do_zmq_send(void *socket,
                          unsigned char *data,
                          unsigned long size,
                          const char *context) {
    while (true) {
      int rc = zmq_send(socket, data, size, 0);
      if (rc == -1) {
        if (errno != EAGAIN) {
          BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to transmit";
          perror(context);
          exit(-1);
        }
        // Retry
      }
      else {
        break;
      }
    }
    return size;
  }

  static
  unsigned long do_zmq_recv(void *socket,
                          unsigned char *data,
                          unsigned long size,
                          const char *context) {
    int rc;
    while (true) {
      rc = zmq_recv(socket, data, size, 0);
      if (rc == -1) {
        if (errno != EAGAIN) {
          BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to receive";
          perror(context);
          exit(-1);
        }
        // Retry
      }
      else {
        break;
      }
    }
    return (unsigned long) rc;
  }

  static
  unsigned long do_zmq_recv_noblock(void *socket,
                                  unsigned char *data,
                                  unsigned long size,
                                  const char *context) {
    int rc;
    rc = zmq_recv(socket, data, size, ZMQ_NOBLOCK);

    if (rc == -1 && errno != EAGAIN) {
      BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to receive";
      perror(context);
      exit(-1);
    }
    return (unsigned long) rc;
  }
}

#endif