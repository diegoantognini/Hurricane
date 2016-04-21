#include "gen-cpp/IODFS.h"
#include "gen-cpp/IODFS_types.h"

#include "../slipstore/interface.hpp"
#include "../slipstore/config.hpp"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/make_shared.hpp>
#include <unordered_map>
#include <string>
#include <vector>

#define NB_SECONDS_BETWEEN_RECONNECTION 1

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using std::unordered_map;
using std::string;
using std::vector;
using std::pair;
using boost::shared_ptr;

class IODFSHandler : virtual public IODFSIf {
  private:
  // IO part
  unordered_map<string, slipstore::io*> local_buffers;
  unordered_map<string, slipstore::io*> global_buffers; //the value is null, but we can use the hash table to find in O(1) :)
  slipstore::slipstore_req_t req;
  
  // Network part
  vector<pair<string, string>> machines;
  unsigned long me;
  unsigned int port;
  bool connection_to_server_done;

  vector<boost::shared_ptr<TTransport>> transports;
  vector<IODFSClient*> clients;

  boost::asio::deadline_timer* reconnect_timer;
  boost::asio::io_service reconnect_thread;
  boost::asio::io_service::work* reconnect_woker;
  boost::thread_group threadpool;

  bool helper_exists(const std::string& filename, unordered_map<string, slipstore::io*>::iterator& it, unordered_map<string, slipstore::io*>& buffers) {
    it = buffers.find(filename);
    return it != buffers.end();
  }

  bool helper_exists(const std::string& filename, unordered_map<string, slipstore::io*>& buffers) {
    return buffers.find(filename) != buffers.end();
  }

  void helper_delete(unordered_map<string, slipstore::io*>::iterator& it) {
    unlink_file((char *)(it->first.c_str()));
    delete it->second;
  }

  void init_req() {
    req.source_mc = req.cmd = req.size = req.offset = 0;
  }

  void reconnect() {
    connection_to_server_done = false;
    unsigned int i = 0;
    try {
      for(auto& transport : transports) {
        if(!transport->isOpen()) {
          transport->open();
          BOOST_LOG_TRIVIAL(info) << "Connection to " << machines[i].first << " SUCCESS !";
        }
        ++i;
      }
      connection_to_server_done = true;
    } catch (TException& tx){
      transports[i]->close();
      BOOST_LOG_TRIVIAL(info) << "Connection to " << machines[i].first << " FAILED : " << tx.what();
      reconnect_timer->expires_at(reconnect_timer->expires_at() + boost::posix_time::seconds(NB_SECONDS_BETWEEN_RECONNECTION));
      reconnect_timer->async_wait(boost::bind(&IODFSHandler::reconnect, this));
    }
  }

  void try_to_reconnect() {
    connection_to_server_done = false;
    reconnect_timer->async_wait(boost::bind(&IODFSHandler::reconnect, this));
    threadpool.create_thread(boost::bind(&boost::asio::io_service::run, &reconnect_thread));
  }

  bool are_all_connection_open(unsigned int* pnode = NULL) {
    bool result = true;

    unsigned int node = 0;
    try {
     for(auto& machine : clients) {
        result = result && machine->nop();
        ++node;
      }
      *pnode = machines.size()+1;
    } catch (TException& tx){
      transports[node]->close();
      *pnode = node;
      BOOST_LOG_TRIVIAL(info) << "Connection to " << machines[node].first << " FAILED : " << tx.what();
      result = false;
    }

    if(!result) {
      connection_to_server_done = false;
      try_to_reconnect();
    }

    return result;
  }

 public:
  IODFSHandler(const vector<pair<string, string>>& _machines, unsigned long _me, unsigned long _port)
  : me(_me), port(_port), connection_to_server_done(false) {
    for(auto& it : _machines)
      machines.push_back(make_pair(it.first, it.second));
    init_req();  

    // Init the network part
    for(auto& machine : machines) {
        boost::shared_ptr<TTransport> socket(new TSocket(machine.first.c_str(), port));
        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        IODFSClient* client = new IODFSClient(protocol);

        transports.push_back(transport);
        clients.push_back(client);
    }

    reconnect_woker = new boost::asio::io_service::work(reconnect_thread);
    // Do the connection later
    reconnect_timer = new boost::asio::deadline_timer(reconnect_thread, boost::posix_time::seconds(NB_SECONDS_BETWEEN_RECONNECTION));
    reconnect_timer->async_wait(boost::bind(&IODFSHandler::reconnect, this));
    threadpool.create_thread(boost::bind(&boost::asio::io_service::run, &reconnect_thread));
  }

  ~IODFSHandler() {
  	for(unordered_map<string, slipstore::io*>::iterator it = local_buffers.begin(); it != local_buffers.end(); ++it)
  		helper_delete(it);
  
    for(auto& it : transports)
      it->close();

    for(unsigned int i = 0; i < clients.size(); ++i)
      delete clients[i];

    reconnect_thread.stop();
    delete reconnect_timer;
    delete reconnect_woker;
  }

  bool nop() {
    return true;
  }

  bool local_create(const std::string& filename) {
    if(local_exists(filename))
      return false;

    local_buffers[filename] = new slipstore::io(filename.c_str(), NULL);
    return true;
  }

  bool local_exists(const std::string& filename) {
  	return helper_exists(filename, local_buffers);
  }

  bool local_delete(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    helper_delete(it);
    local_buffers.erase(it);

    return true;
  }

  void local_fill(std::string& _return, const std::string& filename, const int64_t size) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      _return = "";
    else {
      init_req();
      req.size = size;
      _return = string(size, 0);
      int64_t bytes_read = it->second->fill((unsigned char*) _return.c_str(), &req);

	  // EOF, should return a string smaller and so, its size is modified
      if(bytes_read < size)
        _return = _return.data();
    }
  }

  void local_seek_and_fill(std::string& _return, const std::string& filename, const int64_t size, const int64_t offset) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      _return = "";
    else {
      init_req();
      req.size = size;
      req.offset = offset;
      _return = string(size, 0);
      int64_t bytes_read = it->second->seek_and_fill((unsigned char*) _return.c_str(), &req);

	  // EOF, should return a string smaller and so, its size is modified
      if(bytes_read < size)
        _return = _return.data();
    }
  }

  bool local_drain(const std::string& filename, const std::string& buffer) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    init_req();
    req.size = buffer.size();
    it->second->drain((unsigned char*) buffer.c_str(), &req);
    return true;
  }

  bool local_rewind(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    it->second->rewind();
    return true;
  }

  bool local_trunc(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    it->second->trunc();
    return true;
  }

  bool local_eof(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return true;

    return it->second->eof();
  }

  int64_t local_size(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return -1;

    return it->second->get_size();
  }

  int64_t local_left(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return -1;

    return it->second->get_bytes_left();
  }

  bool global_create(const std::string& filename) {
    unsigned int lost_node;
    if(!connection_to_server_done || !are_all_connection_open(&lost_node)) {
      LostConnection lc;
      lc.name_node_lost = machines[lost_node].first;
      throw lc;
    }

    if(global_exists(filename) || helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    unsigned int node = 0; 
    try {
      for(auto& machine : clients) {
        result = result && machine->local_create(filename);
        ++node;
      }
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[node].first;
      throw lcdc;
    }

    global_buffers[filename] = NULL;

    return result;
  }

  bool global_exists(const std::string& filename) {
    unsigned int lost_node;
    if(!connection_to_server_done || !are_all_connection_open(&lost_node)) {
      LostConnection lc;
      lc.name_node_lost = machines[lost_node].first;
      throw lc;
    }

    bool result = true;
    unsigned int node = 0; 
    try {
      for(auto& machine : clients) {
        result = result && machine->local_exists(filename);
        ++node;
      }
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[node].first;
      throw lcdc;
    }

    return result;
  }

  bool global_delete(const std::string& filename) {
    unsigned int lost_node;
    if(!connection_to_server_done || !are_all_connection_open(&lost_node)) {
      LostConnection lc;
      lc.name_node_lost = machines[lost_node].first;
      throw lc;
    }

    if(!global_exists(filename) || !helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    unsigned int node = 0; 
    try {
      for(auto& machine : clients) {
        result = result && machine->local_delete(filename);
        ++node;
      }
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[node].first;
      throw lcdc;
    }

    return result;
  }

  virtual void global_fill(std::string& _return, const std::string& filename, const int64_t size) {
    // Your implementation goes here
    printf("global_fill\n");
  }

  void global_seek_and_fill(std::string& _return, const std::string& filename, const int64_t size, const int64_t offset) {
    // Your implementation goes here
    printf("global_seek_and_fill\n");
  }

  bool global_drain(const std::string& filename, const std::string& buffer) {
    // Your implementation goes here
    printf("global_drain\n");
    return false;
  }

  bool global_rewind(const std::string& filename) {
    // Your implementation goes here
    printf("global_rewind\n");
    return false;
  }

  bool global_trunc(const std::string& filename) {
    // Your implementation goes here
    printf("global_trunc\n");
    return false;
  }

  bool global_eof(const std::string& filename) {
    // Your implementation goes here
    printf("global_eof\n");
    return false;
  }

  int64_t global_size(const std::string& filename) {
    // Your implementation goes here
    printf("global_size\n");
    return 0;
  }

};

int main(int argc, const char **argv) {
  int port = 9090;
  setup_options(argc, argv);
  init_slipstore_desc();
  unsigned long me = pt_slipstore.get < unsigned long > ("machines.me");

  vector<pair<string, string>> machines;
  for(unsigned int i = 0; i < pt_slipstore.get < unsigned long > ("machines.count"); ++i){
    std::stringstream name;
    std::stringstream iface;
    name << "machines.name" << i;
    iface << "machines.iface" << i;
    machines.push_back(make_pair(pt_slipstore.get<std::string>(name.str().c_str()), pt_slipstore.get<std::string>(iface.str().c_str())));
  } 

  TThreadedServer server(
    boost::make_shared<IODFSProcessor>(boost::make_shared<IODFSHandler>(machines, me, port)),
    boost::make_shared<TServerSocket>(port),
    boost::make_shared<TBufferedTransportFactory>(),
    boost::make_shared<TBinaryProtocolFactory>());
  
  /*
  // if you don't need per-connection state, do the following instead
  TThreadedServer server(
    boost::make_shared<IODFSProcessor>(boost::make_shared<IODFSHandler>(machines, me, port)),
    boost::make_shared<TServerSocket>(port),
    boost::make_shared<TBufferedTransportFactory>(),
    boost::make_shared<TBinaryProtocolFactory>());
  */
  /*
  // This server only allows one connection at a time, but spawns no threads
  TSimpleServer server(
    boost::make_shared<IODFSProcessor>(boost::make_shared<IODFSHandler>(machines, me, port)),
    boost::make_shared<TServerSocket>(9090),
    boost::make_shared<TBufferedTransportFactory>(),
    boost::make_shared<TBinaryProtocolFactory>());
  */

  /*
  const int workerCount = 4;

  boost::shared_ptr<ThreadManager> threadManager =
    ThreadManager::newSimpleThreadManager(workerCount);
  threadManager->threadFactory(
    boost::make_shared<PlatformThreadFactory>());
  threadManager->start();

  // This server allows "workerCount" connection at a time, and reuses threads
  TThreadPoolServer server(
    boost::make_shared<IODFSProcessor>(boost::make_shared<IODFSHandler>(machines, me, port)),
    boost::make_shared<TServerSocket>(9090),
    boost::make_shared<TBufferedTransportFactory>(),
    boost::make_shared<TBinaryProtocolFactory>(),
    threadManager);
  */
  
  server.serve();

  return 0;
}
