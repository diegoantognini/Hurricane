#ifndef _SERVER_ASYNC_
#define _SERVER_ASYNC_

#include "server.hpp"
#include "../interface.hpp"
#include "../../utils/clock_utils.h"

namespace slipstore {
	class server_async : public server {

  public:
      server_async(io *disks_in)
          : server(disks_in) { };

      // Server
      virtual void operator()() {
        rtc_clock total_time;
        rtc_clock busy_time_disk;
        rtc_clock busy_time_mem;
        rtc_clock *current_busy_time;
        setup_conn(); // Sockets should be set up in the same thread
        unsigned char *buffer = new unsigned char[slipchunk];
        bool polling_server = (vm.count("polling_server") > 0);
        // Store previous request (used to overlap IO and network)
        slipstore_req_t prev_req = {0};
        bool use_prev_req = false, skip_ack = false;
        total_time.start();
        busy_time_disk.start();
        current_busy_time = &busy_time_disk;
        while (true) {
          // Get cmd
          slipstore_req_t req;
          unsigned long requestor;
          unsigned long resp;

          // Is there a previous req?
          if (use_prev_req) { // If yes, then req = prev_req
            req = prev_req;
            use_prev_req = false;
          } else { // If no, receive new req
            current_busy_time->stop();
            if (polling_server) {
              long rc;
              do {
                rc = do_zmq_recv_noblock(socket_cmd_in,
                                         (unsigned char *) &req,
                                         sizeof(slipstore_req_t),
                                         "server recv cmd:");
              } while (rc < 0);
            }
            else {
              (void) do_zmq_recv(socket_cmd_in,
                                 (unsigned char *) &req,
                                 sizeof(slipstore_req_t),
                                 "server recv cmd:");
            }
            if (req.cmd == CMD_FILL && req.stream == STREAM_MEMBUFFER) {
              busy_time_mem.start();
              current_busy_time = &busy_time_mem;
            }
            else {
              busy_time_disk.start();
              current_busy_time = &busy_time_disk;
            }
          }

          requestor = req.source_mc;
          if (req.cmd == CMD_TERMINATE) {
            resp = RESP_ACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            BOOST_LOG_TRIVIAL(info) << "SLIPSTORE service is exiting";
            break;
          }
          else if (req.cmd == CMD_NOP) {
            resp = RESP_ACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_EOF) {
            // Check whether stream is empty
            if (disks->eof()) {
              resp = RESP_ACK;
            }
            else {
              resp = RESP_NACK;
            }
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_RESET) {
            // Reset stream
            resp = RESP_ACK;
            disks->trunc();
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          switch (req.cmd) {
            case CMD_FILL:
              if (!disks->eof()) {
                resp = RESP_ACK;
              }
              else {
                resp = RESP_NACK;
              }
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              // If we responded positively then send the data
              if (resp == RESP_ACK) {
                unsigned long size = disks->fill(buffer, &req);
                /*
                unsigned int dummy;
                (void) do_zmq_recv(sockets_data_out[requestor],
                 (unsigned char *)&dummy,
                 sizeof(int),
                 "dummy");
                */
                (void) do_zmq_send(sockets_data_out[requestor],
                                   buffer,
                                   size,
                                   "server data out:");
              }
              break;
            case CMD_SEEK_AND_FILL:
              resp = RESP_ACK;
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              // If we responded positively then send the data
              if (resp == RESP_ACK) {
                unsigned long size = disks->seek_and_fill(buffer, &req);
                /*
                unsigned int dummy;
                (void) do_zmq_recv(sockets_data_out[requestor],
                 (unsigned char *)&dummy,
                 sizeof(int),
                 "dummy");
                */
                (void) do_zmq_send(sockets_data_out[requestor],
                                   buffer,
                                   size,
                                   "server data out:");
              }
              break;
            case CMD_DRAIN:
              // Check whether we have space left
              if (disks->have_drain_space(&req)) {
                resp = RESP_ACK;
              }
              else {
                resp = RESP_NACK;
              }

              if (!skip_ack) {
                (void) do_zmq_send(socket_cmd_in,
                                   (unsigned char *) &resp,
                                   sizeof(unsigned long),
                                   "server resp out:");
              } else {
                skip_ack = false;
              }
              // If we responded positively then receive the data
              if (resp == RESP_ACK) {
                // Read data
                (void) do_zmq_recv(sockets_data_in[requestor],
                                   buffer,
                                   req.size,
                                   "server data in:");

                // Peek
                long rc = do_zmq_recv_noblock(socket_cmd_in,
                                              (unsigned char *) &prev_req,
                                              sizeof(slipstore_req_t),
                                              "server recv cmd:");

                if (rc >= 0) {
                  // We have another request waiting
                  // Check if it's also a drain and we have enough space...
                  if (prev_req.cmd == CMD_DRAIN && disks->have_drain_space(prev_req.size + req.size)) {
                    resp = RESP_ACK;
                    (void) do_zmq_send(socket_cmd_in,
                                       (unsigned char *) &resp,
                                       sizeof(unsigned long),
                                       "server resp out:");
                    skip_ack = true;
                  } else {
                    skip_ack = false;
                  }
                  use_prev_req = true;
                } else {
                  skip_ack = false;
                  use_prev_req = false;
                }

                // Drain
                disks->drain(buffer, &req);
              }
              break;
            default:
              BOOST_LOG_TRIVIAL(fatal) << "Unknown command to slipstore service:"
              << req.cmd;
              exit(-1);
          }
        }
        current_busy_time->stop();
        total_time.stop();
        delete buffer;
        teardown_conn();
        busy_time_disk.print("SLIPSTORE::SERVER_BUSY_DISK ");
        busy_time_mem.print("SLIPSTORE::SERVER_BUSY_MEM ");
        total_time.print("SLIPSTORE::SERVER_TOTAL ");
      }
  };
}

#endif
