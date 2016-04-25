#include "gen-cpp/IODFS.h"
#include "gen-cpp/IODFS_types.h"

#include "../slipstore/interface.hpp"
#include "../slipstore/config.hpp"
#include "../slipstore/io/io.hpp"

#include "../slipstore/server/server_async.hpp"
#include "../slipstore/server/server_sync.hpp"

#include "../slipstore/client/client_fill.hpp"
#include "../slipstore/client/client_drain.hpp"

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
#include <algorithm>

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
using std::random_shuffle;

class IODFSHandler : virtual public IODFSIf {
  private:
  // IO part
  unordered_map<string, slipstore::io*> local_buffers;
  unordered_map<string, slipstore::io*> global_buffers; //the value is null, but we can use the hash table to find in O(1) :)
  slipstore::slipstore_req_t req;
  unsigned long slipchunk;
  unsigned long slipfile_size;

  // Network part
  vector<pair<string, string>> machines;
  unsigned long me;
  unsigned int port;
  bool connection_to_server_done;
  unsigned int first_known_lost_node;

  vector<boost::shared_ptr<TTransport>> transports;
  vector<IODFSClient*> clients;

  boost::asio::deadline_timer* reconnect_timer;
  boost::asio::io_service reconnect_thread;
  boost::asio::io_service::work* reconnect_woker;

 public:
  IODFSHandler(const vector<pair<string, string>>& _machines, unsigned long _me, unsigned long _port, unsigned long _slipchunk, unsigned long _slipfile_size)
  : slipchunk(_slipchunk), slipfile_size(_slipfile_size), me(_me), port(_port), connection_to_server_done(false), first_known_lost_node(_machines.size()) {
    
    for(auto& it : _machines)
      machines.push_back(make_pair(it.first, it.second));

    init_req();  

    slipstore::init(&local_buffers);

    // Shuffling the machine in order that every machine have ~ the same amount of requests
    string me_name = machines[me].first;
    random_shuffle(machines.begin(), machines.end());
    me = machines.size()+1;
    for(unsigned int i = 0; me > machines.size() && i < machines.size(); ++i)
      if(machines[i].first == me_name)
        me = i;

    // Init the network part
    for(auto& machine : machines) {
        boost::shared_ptr<TTransport> socket(new TSocket(machine.first.c_str(), port));
        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        IODFSClient* client = new IODFSClient(protocol);

        transports.push_back(transport);
        clients.push_back(client);
    }

    // Necessary if we want to reuse our thread
    reconnect_woker = new boost::asio::io_service::work(reconnect_thread);

    // Do the connection later
    reconnect_timer = new boost::asio::deadline_timer(reconnect_thread, boost::posix_time::seconds(NB_SECONDS_BETWEEN_RECONNECTION));
    reconnect_timer->async_wait(boost::bind(&IODFSHandler::reconnect, this));
    slipstore::threadpool.create_thread(boost::bind(&boost::asio::io_service::run, &reconnect_thread));
  }

  ~IODFSHandler() {
    slipstore::shutdown();

    reconnect_thread.stop();

  	for(unordered_map<string, slipstore::io*>::iterator it = local_buffers.begin(); it != local_buffers.end(); ++it)
  		helper_delete(it);
  
    for(auto& it : transports)
      it->close();

    for(auto& client : clients)
      delete client;

    delete reconnect_timer;
    delete reconnect_woker;
  }




  ////////////////////////////////////////////
  //////////// LOCAL PART ////////////////////
  ////////////////////////////////////////////




  
  // False indicate that the file already exists
  bool local_create(const std::string& filename) {
    if(local_exists(filename))
      return false;

    local_buffers[filename] = new slipstore::io(filename.c_str(), NULL);
    return true;
  }

  bool local_exists(const std::string& filename) {
  	return helper_exists(filename, local_buffers);
  }

  // False indicates that the file already exists
  bool local_delete(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    helper_delete(it);
    local_buffers.erase(it);

    // Remove from global_buffer if the request come from global
    if(global_buffers.find(filename) != global_buffers.end())
      global_buffers.erase(filename);

    return true;
  }

  // An empty buffer implies either the file already exists or we are at the EOF
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

  // An empty buffer implies either the file already exists or we are at the EOF
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

  // False indicate that the file doesn't exist
  bool local_drain(const std::string& filename, const std::string& buffer) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    init_req();
    req.size = buffer.size();
    it->second->drain((unsigned char*) buffer.c_str(), &req);
    return true;
  }

  // False indicate that the file doesn't exist
  bool local_rewind(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    it->second->rewind();
    return true;
  }

  // False indicate that the file doesn't exist
  bool local_trunc(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    it->second->trunc();
    return true;
  }

  // True indicate that the file doesn't exist of we are EOF
  bool local_eof(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return true;

    return it->second->eof();
  }

  // -1 indicate that the file doesn't exist
  int64_t local_size(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return -1;

    return it->second->get_size();
  }

  // -1 indicate that the file doesn't exist
  int64_t local_left(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return -1;

    return it->second->get_bytes_left();
  }




  ////////////////////////////////////////////
  //////////// GLOBAL PART ///////////////////
  ////////////////////////////////////////////



  // Needed in order to know if a connected machine is still available
  bool nop() {
    return true;
  }

  // False implie that the file already exist on this machine
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  bool global_create(const std::string& filename) {
    handle_connection_to_server_done(true);

    if(global_exists(filename) || helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_create(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].first;
      throw lcdc;
    }

    global_buffers[filename] = NULL;

    return result;
  }

  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  bool global_exists(const std::string& filename) {
    handle_connection_to_server_done(true);

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_exists(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].first;
      throw lcdc;
    }

    return result;
  }

  // False implie that the file doesn't exist on this machine or on another machine
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  bool global_delete(const std::string& filename) {
    handle_connection_to_server_done(true);
    if(!global_exists(filename) || !helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_delete(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].first;
      throw lcdc;
    }

    return result;
  }

  void global_fill(std::string& _return, const std::string& filename, const int64_t size) {
    handle_connection_to_server_done(false);

    if(!global_exists(filename) || !helper_exists(filename, global_buffers))
      _return = "";
    else {
      init_req();
      strcpy(req.filename, filename.c_str());
      req.cmd = slipstore::CMD_FILL;
      req.size = size;

      _return = string(size, 0);
      unsigned long filled_bytes = slipstore::slipstore_client_fill->batch_access_store(&req, (unsigned char*) _return.c_str(), slipchunk, size);

      // EOF, should return a string smaller and so, its size is modified
      if(filled_bytes < ((unsigned long)size))
          _return = _return.data();
    }
  }


  bool global_drain(const std::string& filename, const std::string& buffer) {
    handle_connection_to_server_done(false);

    if(!global_exists(filename) || !helper_exists(filename, global_buffers))
      return false;

    init_req();
    strcpy(req.filename, filename.c_str());
    req.cmd = slipstore::CMD_DRAIN;
    
    if (slipstore::slipstore_client_drain->batch_access_store(&req, (unsigned char*) buffer.c_str(), slipchunk, buffer.size())) {
      BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
      return false;
    }

    return true;
  }

  // False implie that the file doesn't exist on this machine or on another machine
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  bool global_rewind(const std::string& filename) {
    handle_connection_to_server_done();

    if(!global_exists(filename) || !helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_rewind(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].first;
      throw lcdc;
    }

    return result;
  }

  // False implie that the file doesn't exist on this machine or on another machine
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  bool global_trunc(const std::string& filename) {
    handle_connection_to_server_done();

    if(!global_exists(filename) || !helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_trunc(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].first;
      throw lcdc;
    }

    return result;
  }

  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  bool global_eof(const std::string& filename) {
    handle_connection_to_server_done();

    if(!global_exists(filename) || !helper_exists(filename, global_buffers))
      return true;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_eof(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].first;
      throw lcdc;
    }

    return result;
  }

  // -1 indicate that on a machine, the file doesn't exist
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  int64_t global_size(const std::string& filename) {
    handle_connection_to_server_done();

    if(!global_exists(filename) || !helper_exists(filename, global_buffers))
      return -1;

    int64_t result = 0;
    try {
      int64_t size;
      for(first_known_lost_node = 0; result >= 0 && first_known_lost_node < clients.size(); ++first_known_lost_node) {
        size = clients[first_known_lost_node]->local_size(filename);
        if(size < 0)
          result = -1;
        else
          result += size;
      }
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].first;
      throw lcdc;
    }

    return result;
  }




  ////////////////////////////////////////////
  //////////// PRIVATE PART //////////////////
  ////////////////////////////////////////////



  
private:
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
    slipstore::threadpool.create_thread(boost::bind(&boost::asio::io_service::run, &reconnect_thread));
  }

  bool are_all_connection_open() {
    bool result = true;

    first_known_lost_node = 0;
    try {
     for(auto& machine : clients) {
        result = result && machine->nop();
        ++first_known_lost_node;
      }
      first_known_lost_node = machines.size()+1;
    } catch (TException& tx){
      transports[first_known_lost_node]->close();
      BOOST_LOG_TRIVIAL(info) << "Connection to " << machines[first_known_lost_node].first << " FAILED : " << tx.what();
      result = false;
    }

    if(!result) {
      connection_to_server_done = false;
      try_to_reconnect();
    }

    return result;
  }

  // true implies doing a voting phase
  void handle_connection_to_server_done(bool with_are_all_connection_open = false) {
     if(!connection_to_server_done || (with_are_all_connection_open && !are_all_connection_open())) {
      LostConnection lc;
      lc.name_node_lost = first_known_lost_node < machines.size() ? machines[first_known_lost_node].first : "Server hasn't been initialised";
      throw lc;
    }
  }
};





  ////////////////////////////////////////////
  //////////// MAIN ////// ///////////////////
  ////////////////////////////////////////////



  
int main(int argc, const char **argv) {
  int port = 9090;
  setup_options(argc, argv);
  init_slipstore_desc();
  unsigned long me = pt_slipstore.get < unsigned long > ("machines.me");

  unsigned long slipchunk = vm["slipchunk"].as < unsigned long > ();
  unsigned long slipfile_size = vm["slipbench_fsize"].as < unsigned long > ();
  unsigned long align = vm["slipbench_align"].as < unsigned long > ();
  slipchunk = (slipchunk / align) * align;

  vector<pair<string, string>> machines;
  for(unsigned int i = 0; i < pt_slipstore.get < unsigned long > ("machines.count"); ++i){
    std::stringstream name;
    std::stringstream iface;
    name << "machines.name" << i;
    iface << "machines.iface" << i;
    machines.push_back(make_pair(pt_slipstore.get<std::string>(name.str().c_str()), pt_slipstore.get<std::string>(iface.str().c_str())));
  } 

  bool batching = (vm.count("request_batching") > 0);
  unsigned long batch_size = vm["batch_size"].as < unsigned long > ();

  BOOST_LOG_TRIVIAL(info) << "BATCHING : " << batching;
  BOOST_LOG_TRIVIAL(info) << "SLIPCHUNK" << slipchunk;
  BOOST_LOG_TRIVIAL(info) << "BATCH SIZE" << batch_size;

  TThreadedServer server(
    boost::make_shared<IODFSProcessor>(boost::make_shared<IODFSHandler>(machines, me, port, slipchunk, slipfile_size)),
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
