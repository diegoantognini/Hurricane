#include "gen-cpp/IODFS.h"
#include "../slipstore/interface.hpp"
#include "../slipstore/config.hpp"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <unordered_map>
#include <string>
#include <vector>

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
  unordered_map<string, slipstore::io*> local_buffers;
  unordered_map<string, slipstore::io*> global_buffers;
  slipstore::slipstore_req_t req;
  vector<pair<string, string>> machines;
  unsigned long me;
  unsigned int port;

  bool helper_exists(const std::string& filename, unordered_map<string, slipstore::io*>::iterator& it, unordered_map<string, slipstore::io*>& buffer) {
    it = buffer.find(filename);
    return it != buffer.end();
  }

  void helper_delete(unordered_map<string, slipstore::io*>::iterator& it) {
    unlink_file((char *)(it->first.c_str()));
    delete it->second;
  }

  void init_req() {
    req.source_mc = req.cmd = req.size = req.offset = 0;
  }

 public:
  IODFSHandler(const vector<pair<string, string>>& _machines, unsigned long _me, unsigned long _port) : me(_me), port(_port) {
    for(auto& it : _machines)
      machines.push_back(make_pair(it.first, it.second));
    init_req();  
  }

  ~IODFSHandler() {
  	for(unordered_map<string, slipstore::io*>::iterator it = local_buffers.begin(); it != local_buffers.end(); ++it)
  		helper_delete(it);
    for(unordered_map<string, slipstore::io*>::iterator it = global_buffers.begin(); it != global_buffers.end(); ++it)
      helper_delete(it);
  }

  bool local_create(const std::string& filename) {
    if(local_exists(filename))
      return false;

    local_buffers[filename] = new slipstore::io(filename.c_str(), NULL);
    return true;
  }

  bool local_exists(const std::string& filename) {
  	return local_buffers.find(filename) != local_buffers.end();
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
    // Your implementation goes here
    printf("global_create\n");
    return false;
  }

  bool global_exists(const std::string& filename) {
    // Your implementation goes here
    printf("global_exists\n");
    return false;
  }

  bool global_delete(const std::string& filename) {
    // Your implementation goes here
    printf("global_delete\n");
    return false;
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

  shared_ptr<IODFSHandler> handler(new IODFSHandler(machines, me, port));
  shared_ptr<TProcessor> processor(new IODFSProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}










