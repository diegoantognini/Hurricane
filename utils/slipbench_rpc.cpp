#include "../RPC/gen-cpp/IODFS.h"
#include "../RPC/gen-cpp/IODFS_types.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include "../hurricane/interface.hpp"
#include "../hurricane/config.hpp"
#include <sys/mman.h>
#include <vector>
#include <algorithm>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using std::unordered_map;
using std::string;
using std::vector;

const unsigned long BIG_BUFSIZE = 256 * 1024 * 1024;
#define NB_THREADS 2

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
      buffer = (unsigned char *) map_anon_memory(BIG_BUFSIZE, true, "Dummy buf");
    }

    void operator()() {
      (void) __sync_fetch_and_add(&bthread_running, 1UL);
      while (true) {
        for (unsigned long i = 0; i < BIG_BUFSIZE; i++) {
          buffer[i]++;
        }
      }
    }
};

class Drain {
public:
    IODFSClient* client;
    unsigned long size_per_thread;
    string* token;
    string* big_buffer;
    string* filename;
    int id;

    Drain(IODFSClient* _client, unsigned long _size_per_thread, string* _token, string* _big_buffer, string* _filename, int _id)
    :client(_client),
    size_per_thread(_size_per_thread),
    token(_token),
    big_buffer(_big_buffer),
    filename(_filename),
    id(_id) {
    }

    void operator()() {
      unsigned long drain_bytes = size_per_thread;
      while (drain_bytes) {
          unsigned long bytes_to_drain = drain_bytes;
          if (bytes_to_drain > BIG_BUFSIZE) {
            bytes_to_drain = BIG_BUFSIZE;
          }

          if(!client->global_drain(*filename, *big_buffer, *token)) {
            BOOST_LOG_TRIVIAL(fatal) << "Unable to write to hurricane";
            exit(-1);
          }
          
          drain_bytes -= bytes_to_drain;
        }
    }
};

class Fill {
public:
    IODFSClient* client;
    unsigned long size_per_thread;
    string* token;
    string* filename;
    unsigned long* fill_bytes;
    int id;

    Fill(IODFSClient* _client, unsigned long _size_per_thread, string* _token, string* _filename, unsigned long* _fill_bytes, int _id)
    :client(_client),
    size_per_thread(_size_per_thread),
    token(_token),
    filename(_filename),
    fill_bytes(_fill_bytes),
    id(_id) {
    }

    void operator()() {
      unsigned long filled_bytes = 0;
      *fill_bytes = 0;
      string big_buffer;
      unsigned long iter = size_per_thread/BIG_BUFSIZE;
      for(unsigned long i = 0; i < iter; ++i) {
        big_buffer = "";
        client->global_fill(big_buffer, *filename, BIG_BUFSIZE, *token);
        filled_bytes = big_buffer.size();
        *fill_bytes += filled_bytes;      
      }
    }
};

void write_into_string(char* in, const char* out, int n) {
  strncpy(in, out, n-1);
  *(in+n) = '\0';
}

int main(int argc, const char **argv) {
  rtc_clock fill_clock;
  rtc_clock drain_clock;

  /* Get options */
  setup_options(argc, argv);
  unsigned long slipchunk = vm["slipchunk"].as < unsigned long > ();
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::SLIPCHUNK " << slipchunk;
  unsigned long slipfile_size = vm["slipbench_fsize"].as < unsigned long > ();
  unsigned long size_per_thread = slipfile_size/NB_THREADS;
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::SLIPBENCH_FIZE " << slipfile_size;
  unsigned long background_threads = vm["slipbench_background_threads"].as < unsigned long > ();
  unsigned long align = vm["slipbench_align"].as < unsigned long > ();
  slipchunk = (slipchunk / align) * align;
  unsigned long blockmem = vm["blocked_memory"].as < unsigned long > ();

  /* Read in hurricane information */
  init_hurricane_desc();
  bool run_drain_test = (vm.count("drain_test") > 0);
  bool run_fill_test = (vm.count("fill_test") > 0);

  unsigned long big_bufsize = BIG_BUFSIZE;

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

  string big_buffer(big_bufsize, 0);
  const char* block = (const char *) map_anon_memory(blockmem, true, "Blockmem");
  write_into_string((char*) big_buffer.c_str(), block, big_bufsize);
  
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

  hurricane::init(NULL, true, false);
  hurricane::hurricane_client_barrier = new hurricane::client_barrier();

  std::stringstream name;
  unsigned long me = pt_hurricane.get < unsigned long > ("machines.me");
  name << "machines.name" << me;
  string server = pt_hurricane.get<string>(name.str().c_str()); // We connect to "our" server
  int port = 9090;
  
  vector<boost::shared_ptr<TTransport>> sockets;
  vector<boost::shared_ptr<TTransport>> transports;
  vector<boost::shared_ptr<TProtocol>> protocols;
  vector<IODFSClient*> clients;
  vector<boost::thread*> threads;

  for(int i = 0; i < NB_THREADS; ++i) {
    boost::shared_ptr<TTransport> socket(new TSocket(server, port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    
    sockets.push_back(socket);
    transports.push_back(transport);
    protocols.push_back(protocol);
    clients.push_back(new IODFSClient(protocol));

    transport->open();
  }

  string filename = "fill_data";
  string token;
  clients.front()->generate_token(token);
  if(!clients.front()->connect(token)){
    BOOST_LOG_TRIVIAL(fatal) << "Unable to connect to hurricane";
    exit(-1);
  }

  control_barrier(); // Start

  if (run_drain_test) {
    if(me == 0)
      if(!clients.front()->global_create(filename, token)) {
        BOOST_LOG_TRIVIAL(fatal) << "Unable to create a file on hurricane";
        exit(-1);
      }
    
    control_barrier(); // Sync with other client
    
    //big_buffer.resize(big_bufsize); // Cannot send a lot of data at once

    BOOST_LOG_TRIVIAL(info) << "Starting drain test";
    
    drain_clock.start();
    if (batching) {
      for(int i = 0; i < NB_THREADS; ++i)
        threads.push_back(new boost::thread(boost::ref(*new Drain(clients[i], size_per_thread, &token, &big_buffer, &filename, i))));
    
      for(auto& thread : threads)
        thread->join();
    }
    else {
      // Not handled
    }
    drain_clock.stop();
    BOOST_LOG_TRIVIAL(info) << "Completed drain test";
    threads.clear();

    control_barrier();

    if(!clients.front()->global_rewind(filename, token)) {
      BOOST_LOG_TRIVIAL(fatal) << "Unable to rewind the file";
      exit(-1);
    }

    control_barrier();
  }

  vector<unsigned long> v_fill_bytes;
  for(int i = 0; i < NB_THREADS; ++i)
    v_fill_bytes.push_back(0);

  control_barrier();

  if (run_fill_test) {
    BOOST_LOG_TRIVIAL(info) << "Starting fill test";
    fill_clock.start();
    if (batching) {
      for(int i = 0; i < NB_THREADS; ++i)
        threads.push_back(new boost::thread(boost::ref(*new Fill(clients[i], size_per_thread, &token, &filename, &(v_fill_bytes[i]), i))));
    
      for(auto& thread : threads)
        thread->join();
    }
    else {
      // Not handled
    }
    fill_clock.stop();
    BOOST_LOG_TRIVIAL(info) << "Completed fill test";
    threads.clear();
  }

  control_barrier();

  if(!clients.front()->leave(token)){
    BOOST_LOG_TRIVIAL(fatal) << "Unable to leave to hurricane";
    exit(-1);
  }

  control_barrier(); // End

  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::DRAIN_BYTES " << slipfile_size;
  drain_clock.print("SLIPBENCH::DRAIN_TIME ");

  unsigned long tot_fill_bytes = 0;
  for(auto& fill_bytes : v_fill_bytes)
    tot_fill_bytes += fill_bytes;

  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::FILL_BYTES " << tot_fill_bytes;
  fill_clock.print("SLIPBENCH::FILL_TIME ");

  for(auto& transport : transports)
    transport->close();

  hurricane::hurricane_client_barrier->teardown_conn();

  hurricane::shutdown(true, false);

  delete hurricane::hurricane_client_barrier;
  
  for(auto& client : clients)
    delete client;

  return 0;
}
