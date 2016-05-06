#include "../RPC/gen-cpp/IODFS.h"
#include "../RPC/gen-cpp/IODFS_types.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include "../hurricane/interface.hpp"
#include "../hurricane/config.hpp"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using std::unordered_map;
using std::string;

class null_barrier_work : public hurricane::barrier_work {
  public:
      virtual void operator()() {
      }
  };

void control_barrier() {
  null_barrier_work null_obj;
  hurricane::hurricane_client_barrier->in_progress = true;
  hurricane::ioService.post
      (boost::bind(&hurricane::client_barrier::work_barrier,
                   hurricane::hurricane_client_barrier,
                   &null_obj));
  while (hurricane::hurricane_client_barrier->in_progress);
}

volatile unsigned long bthread_running = 0;

class interference {
    unsigned char *buffer;
public:
    interference() {
      buffer = (unsigned char *) map_anon_memory(512 * 1024 * 1024, true, "Dummy buf");
    }

    void operator()() {
      (void) __sync_fetch_and_add(&bthread_running, 1UL);
      while (true) {
        for (unsigned long i = 0; i < 512 * 1024 * 1024; i++) {
          buffer[i]++;
        }
      }
    }
};

int main(int argc, const char **argv) {
  const char* server = "128.178.52.205"; // Connect to one of the server
  int port = 9090;

  boost::shared_ptr<TTransport> socket(new TSocket(server, port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  IODFSClient client(protocol);

  transport->open();

  rtc_clock fill_clock;
  rtc_clock drain_clock;

  /* Get options */
  setup_options(argc, argv);
  unsigned long slipchunk = vm["slipchunk"].as < unsigned long > ();
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::SLIPCHUNK " << slipchunk;
  unsigned long slipfile_size = vm["slipbench_fsize"].as < unsigned long > ();
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::SLIPBENCH_FIZE " << slipfile_size;
  unsigned long background_threads = vm["slipbench_background_threads"].as < unsigned long > ();
  unsigned long align = vm["slipbench_align"].as < unsigned long > ();
  slipchunk = (slipchunk / align) * align;
  unsigned long blockmem = vm["blocked_memory"].as < unsigned long > ();

  /* Read in hurricane information */
  init_hurricane_desc();
  bool run_drain_test = (vm.count("drain_test") > 0);
  bool run_fill_test = (vm.count("fill_test") > 0);
  string filename = "fill_data";

  string token;
  client.generate_token(token);
  if(!client.connect(token)){
    BOOST_LOG_TRIVIAL(fatal) << "Unable to connect to hurricane";
    exit(-1);
  }

  unsigned char *buffer = new unsigned char[slipchunk];
  unsigned long fill_bytes = 0;
  unsigned long big_bufsize = 512 * 1024 * 1024;

  if (blockmem > 0) {
    BOOST_LOG_TRIVIAL(info) << "Blocking " << blockmem;
  }
  else {
    blockmem = big_bufsize;
    BOOST_LOG_TRIVIAL(info) << "Allocating " << blockmem;
  }
  if (blockmem < big_bufsize) {
    blockmem = big_bufsize;
  }

  string big_buffer((char *) map_anon_memory(blockmem, true, "Blockmem"));

  BOOST_LOG_TRIVIAL(info) << "Done ";

  bool batching = (vm.count("request_batching") > 0);
  BOOST_LOG_TRIVIAL(info) << "Batching : " << batching << ", " << vm["batch_size"].as < unsigned long > ();
  
  if (background_threads > 0) {
    BOOST_LOG_TRIVIAL(info) << "Starting background threads";
    for (unsigned long i = 0; i < background_threads; i++) {
      (void) new boost::thread(boost::ref(*new interference()));
    }
    while (bthread_running != background_threads);
    BOOST_LOG_TRIVIAL(info) << "done";
  }

  control_barrier(); // everyone started
  if (run_drain_test) {
    BOOST_LOG_TRIVIAL(info) << "Starting drain test";
    drain_clock.start();
    unsigned long drain_bytes = slipfile_size;
    if (batching) {
      while (drain_bytes) {
        unsigned long bytes_to_drain = drain_bytes;
        if (bytes_to_drain > big_bufsize) {
          bytes_to_drain = big_bufsize;
        }

        if(!client.global_drain(filename, big_buffer, token)) {
          BOOST_LOG_TRIVIAL(fatal) << "Unable to write to hurricane";
          exit(-1);
        }
        
        drain_bytes -= bytes_to_drain;
      }
    }
    else {
      // Not handled
    }
    drain_clock.stop();
    BOOST_LOG_TRIVIAL(info) << "Completed drain test";
    if (run_fill_test) {
      control_barrier();
      if(!client.global_rewind(filename, token)) {
        BOOST_LOG_TRIVIAL(fatal) << "Unable to rewind the file";
        exit(-1);
      }
      control_barrier();
    }
  }

  /*if (run_fill_test) {
    disks->clear();
    strcpy(req.filename, "fill_test_data");
    (*disks)[string(req.filename)] = streams;
    streams->fill_test_prep(req.filename);
    control_barrier();
    BOOST_LOG_TRIVIAL(info) << "Starting fill test";
    fill_clock.start();
    if (batching) {
      unsigned long filled_bytes;
      do {
        req.cmd = hurricane::CMD_FILL;
        filled_bytes =
            hurricane::hurricane_client_fill->batch_access_store(&req,
                                                                 big_buffer,
                                                                 slipchunk,
                                                                 big_bufsize);
        fill_bytes += filled_bytes;
      } while (filled_bytes > 0);
    }
    else {
      do {
        req.cmd = hurricane::CMD_FILL;
        req.size = slipchunk;
        if (!hurricane::hurricane_client_fill->access_store(&req, buffer)) {
          break;
        }
        fill_bytes += req.size;
      } while (true);
    }
    fill_clock.stop();
    BOOST_LOG_TRIVIAL(info) << "Completed fill test";
  }*/

  control_barrier(); // everyone done

  delete buffer;
  
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::DRAIN_BYTES " << slipfile_size;
  drain_clock.print("SLIPBENCH::DRAIN_TIME ");
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::FILL_BYTES " << fill_bytes;
  fill_clock.print("SLIPBENCH::FILL_TIME ");


  transport->close();

  return 0;
}
