#ifndef _SERVER_SYNC_
#define _SERVER_SYNC_

#include "server.hpp"

namespace slipstore {
class server_sync : public server {
  public:
      server_sync(io *disks_in,
                  unsigned long tile_size_in,
                  unsigned long partitions_in)
          : server(disks_in, tile_size_in, partitions_in) { };

      virtual void operator()() {
        // TODO
        //setup_conn(); // Sockets should be set up in the same thread
        //unsigned char *buffer = new unsigned char[slipchunk];
        //rtc_clock total_time;
        //rtc_clock busy_time_disk;
        //rtc_clock busy_time_mem;
        //rtc_clock *current_busy_time;
        //bool polling_server = (vm.count("polling_server") > 0);
        //total_time.start();
        //busy_time_disk.start();
        //current_busy_time = &busy_time_disk;
        //while (true) {
        //  // Get cmd
        //  slipstore_req_t req;
        //  unsigned long requestor;
        //  unsigned long resp;
        //  current_busy_time->stop();
        //  if (polling_server) {
        //    long rc;
        //    do {
        //      rc = do_zmq_recv_noblock(socket_cmd_in,
        //                               (unsigned char *) &req,
        //                               sizeof(slipstore_req_t),
        //                               "server recv cmd:");
        //    } while (rc < 0);
        //  }
        //  else {
        //    (void) do_zmq_recv(socket_cmd_in,
        //                       (unsigned char *) &req,
        //                       sizeof(slipstore_req_t),
        //                       "server recv cmd:");
        //  }
        //  if (req.cmd == CMD_FILL && req.stream == STREAM_MEMBUFFER) {
        //    busy_time_mem.start();
        //    current_busy_time = &busy_time_mem;
        //  }
        //  else {
        //    busy_time_disk.start();
        //    current_busy_time = &busy_time_disk;
        //  }
        //  requestor = req.source_mc;
        //  if (req.cmd == CMD_TERMINATE) {
        //    resp = RESP_ACK;
        //    (void) do_zmq_send(socket_cmd_in,
        //                       (unsigned char *) &resp,
        //                       sizeof(unsigned long),
        //                       "server resp out:");
        //    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE service is exiting";
        //    break;
        //  }
        //  else if (req.cmd == CMD_NOP) {
        //    resp = RESP_ACK;
        //    (void) do_zmq_send(socket_cmd_in,
        //                       (unsigned char *) &resp,
        //                       sizeof(unsigned long),
        //                       "server resp out:");
        //    continue;
        //  }
        //  else if (req.cmd == CMD_ORDER_INIT) {
        //    help_stuff->init_order(req.stream);
        //    resp = RESP_ACK;
        //    (void) do_zmq_send(socket_cmd_in,
        //                       (unsigned char *) &resp,
        //                       sizeof(unsigned long),
        //                       "server resp out:");
        //    continue;
        //  }
        //  else if (req.cmd == CMD_SEMUP) {
        //    resp = RESP_ACK;
        //    help_stuff->sem_up(req.source_mc);
        //    (void) do_zmq_send(socket_cmd_in,
        //                       (unsigned char *) &resp,
        //                       sizeof(unsigned long),
        //                       "server resp out:");
        //    continue;
        //  }
        //  else if (req.cmd == CMD_OFFER_HELP) {
        //    resp = help_stuff->make_decision(&req);
        //    (void) do_zmq_send(socket_cmd_in,
        //                       (unsigned char *) &resp,
        //                       sizeof(unsigned long),
        //                       "server resp out:");
        //    continue;
        //  }
        //  else if (req.cmd == CMD_EOF) {
        //    // Check whether stream is empty
        //    if (disks->eof_stream(&req)) {
        //      resp = RESP_ACK;
        //    }
        //    else {
        //      resp = RESP_NACK;
        //    }
        //    (void) do_zmq_send(socket_cmd_in,
        //                       (unsigned char *) &resp,
        //                       sizeof(unsigned long),
        //                       "server resp out:");
        //    continue;
        //  }
        //  else if (req.cmd == CMD_RESET) {
        //    // Reset stream
        //    resp = RESP_ACK;
        //    disks->trunc(req.stream, req.partition, req.tile);
        //    (void) do_zmq_send(socket_cmd_in,
        //                       (unsigned char *) &resp,
        //                       sizeof(unsigned long),
        //                       "server resp out:");
        //    continue;
        //  }
        //  // Check size of request
        //  if (req.size > slipchunk && req.cmd != CMD_ORDER_NEXT) {
        //    BOOST_LOG_TRIVIAL(warn)
        //    << "SLIPSTORE: Rejecting request sized = "
        //    << req.size
        //    << " larger than slipchunk = "
        //    << slipchunk
        //    << " from: " << req.source_mc;
        //    resp = RESP_NACK;
        //    (void) do_zmq_send(socket_cmd_in,
        //                       (unsigned char *) &resp,
        //                       sizeof(unsigned long),
        //                       "server resp out:");
        //    continue;
        //  }
        //  switch (req.cmd) {
        //    case CMD_ORDER_NEXT:
        //      resp = RESP_ACK;
        //      (void) do_zmq_send(socket_cmd_in,
        //                         (unsigned char *) &resp,
        //                         sizeof(unsigned long),
        //                         "server resp out:");
        //      *(unsigned long *) buffer = help_stuff->order_next(req.partition);
        //      (void) do_zmq_send(sockets_data_out[requestor],
        //                         buffer,
        //                         sizeof(unsigned long),
        //                         "server data out:");
        //      break;
        //    case CMD_FILL:
        //      if (req.stream == STREAM_MEMBUFFER || !disks->eof(&req)) {
        //        resp = RESP_ACK;
        //      }
        //      else {
        //        resp = RESP_NACK;
        //      }
        //      (void) do_zmq_send(socket_cmd_in,
        //                         (unsigned char *) &resp,
        //                         sizeof(unsigned long),
        //                         "server resp out:");
        //      // If we responded positively then send the data
        //      if (resp == RESP_ACK) {
        //        unsigned long size = disks->fill(buffer, &req);
        //        /*unsigned int dummy;
        //        (void) do_zmq_recv(sockets_data_out[requestor],
        //         (unsigned char *)&dummy,
        //         sizeof(int),
        //         "dummy");
        //        */
        //        (void) do_zmq_send(sockets_data_out[requestor],
        //                           buffer,
        //                           size,
        //                           "server data out:");
        //      }
        //      break;
        //    case CMD_SEEK_AND_FILL:
        //      resp = RESP_ACK;
        //      (void) do_zmq_send(socket_cmd_in,
        //                         (unsigned char *) &resp,
        //                         sizeof(unsigned long),
        //                         "server resp out:");
        //      // If we responded positively then send the data
        //      if (resp == RESP_ACK) {
        //        unsigned long size = disks->seek_and_fill(buffer, &req);
        //        /*
        //        unsigned int dummy;
        //        (void) do_zmq_recv(sockets_data_out[requestor],
        //         (unsigned char *)&dummy,
        //         sizeof(int),
        //         "dummy");
        //        */
        //        (void) do_zmq_send(sockets_data_out[requestor],
        //                           buffer,
        //                           size,
        //                           "server data out:");
        //      }
        //      break;
        //    case CMD_DRAIN:
        //      // Check whether we have space left
        //      if (disks->have_drain_space(&req)) {
        //        resp = RESP_ACK;
        //      }
        //      else {
        //        resp = RESP_NACK;
        //      }
        //      (void) do_zmq_send(socket_cmd_in,
        //                         (unsigned char *) &resp,
        //                         sizeof(unsigned long),
        //                         "server resp out:");
        //      // If we responded positively then receive the data
        //      if (resp == RESP_ACK) {
        //        (void) do_zmq_recv(sockets_data_in[requestor],
        //                           buffer,
        //                           req.size,
        //                           "server data in:");
        //        disks->drain(buffer, &req);
        //      }
        //      break;
        //    default:
        //      BOOST_LOG_TRIVIAL(fatal) << "Unknown command to slipstore service:"
        //      << req.cmd;
        //      exit(-1);
        //  }
        //}
        //current_busy_time->stop();
        //total_time.stop();
        //delete buffer;
        //teardown_conn();
        //help_stuff->dump_stats();
        //busy_time_disk.print("SLIPSTORE::SERVER_BUSY_DISK ");
        //busy_time_mem.print("SLIPSTORE::SERVER_BUSY_MEM ");
        //total_time.print("SLIPSTORE::SERVER_TOTAL ");
      }
  };	
}

#endif