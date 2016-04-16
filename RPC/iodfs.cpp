#include "gen-cpp/IODFS.h"
#include "../slipstore/interface.hpp"
#include "../slipstore/config.hpp"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <unordered_map>
#include <string>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using std::unordered_map;
using std::string;

using boost::shared_ptr;

class IODFSHandler : virtual public IODFSIf {
  private:
  unordered_map<string, slipstore::io*> local_buffers;
  slipstore::slipstore_req_t req;

  bool local_exists(const std::string& filename, unordered_map<string, slipstore::io*>::iterator& it) {
    it = local_buffers.find(filename);
    return it != local_buffers.end();
  }

  void init_req() {
    req.source_mc = req.cmd = req.size = req.offset = 0;
  }

 public:
  IODFSHandler() {
    init_req();  
  }

  ~IODFSHandler() {
  	for(auto& it : local_buffers)
  		delete it.second;
  }

  bool local_create(const std::string& filename) {
    if(!local_exists(filename))
      return false;
    
    local_buffers[filename] = new slipstore::io(filename.c_str(), NULL);
    return true;
  }

  bool local_exists(const std::string& filename) {
    return local_buffers.find(filename) != local_buffers.end();
  }

  bool local_delete(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
      return false;
    
    delete it->second;
    local_buffers.erase(it);

    return true;
  }

  //We assume that _return has already the size = #bytes to read (e.g string s1(100,0)) (0 are really important) -> C++11
  void local_fill(std::string& _return, const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
      _return = "";
    else {
      unsigned long bytes_to_read = _return.size();

      init_req();
      req.size = bytes_to_read;
      unsigned long bytes_read = it->second->fill((unsigned char*) _return.c_str(), &req);

      // EOF, should return a string smaller and so, its size is modified
      if(bytes_read < bytes_to_read)
        _return = _return.data();
    }
  }
  //We assume that _return has already the size = #bytes to read (e.g string s1(100,0)) (0 are really important) -> C++11
  void local_seek_and_fill(std::string& _return, const std::string& filename, const int64_t offset) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
      _return = "";
    else {
      unsigned long bytes_to_read = _return.size();

      init_req();
      req.size = bytes_to_read;
      req.offset = offset;
      unsigned long bytes_read = it->second->seek_and_fill((unsigned char*) _return.c_str(), &req);

      // EOF, should return a string smaller and so, its size is modified
      if(bytes_read < bytes_to_read)
        _return = _return.data();
    }
  }

  bool local_drain(const std::string& filename, const std::string& buffer) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
      return false;

    init_req();
    req.size = buffer.size();
    it->second->drain((unsigned char*) buffer.c_str(), &req);
    return true;
  }

  bool local_rewind(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
      return false;

    it->second->rewind();
    return true;
  }

  bool local_trunc(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
      return false;

    it->second->trunc();
    return true;
  }

  bool local_eof(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
      return true;

    return it->second->eof();
  }

  int64_t local_size(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
      return -1;

    return it->second->get_size();
  }

  int64_t local_left(const std::string& filename) {
    unordered_map<string, slipstore::io*>::iterator it;
    if(!local_exists(filename, it))
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

  void global_fill(std::string& _return, const std::string& filename) {
    // Your implementation goes here
    printf("global_fill\n");
  }

  void global_seek_and_fill(std::string& _return, const std::string& filename, const int64_t offset) {
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

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<IODFSHandler> handler(new IODFSHandler());
  shared_ptr<TProcessor> processor(new IODFSProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

