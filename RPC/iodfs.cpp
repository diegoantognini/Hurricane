#include "gen-cpp/IODFS.h"
#include "gen-cpp/IODFS_types.h"

#include "../hurricane/interface.hpp"
#include "../hurricane/config.hpp"
#include "../hurricane/io/io.hpp"

#include "../hurricane/server/server_async.hpp"
#include "../hurricane/server/server_sync.hpp"

#include "../hurricane/client/client_fill.hpp"
#include "../hurricane/client/client_drain.hpp"

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
#include <mutex>

#define NB_SECONDS_BETWEEN_RECONNECTION 1
#define LENGTH_TOKEN  32
#define PORT 9090

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using std::unordered_map;
using std::stringstream;
using std::string;
using std::vector;
using std::pair;
using boost::shared_ptr;
using std::random_shuffle;
using std::mutex;

class IODFSHandler : virtual public IODFSIf {
  private:
  // IO part
  unordered_map<string, hurricane::io*> local_buffers;
  unordered_map<string, hurricane::io*> global_buffers; //the value is null, but we can use the hash table to find in O(1) :)
  hurricane::hurricane_req_t req;
  unsigned long slipchunk;

  // Network part
  vector<network_interface> machines; //Struct defined in IODFS_types.h
  mutex add_machines;
  unordered_map<string, double*> tokens; //Same tricks as global_buffers
  unordered_map<string, hurricane::client_drain*> client_drains;
  unordered_map<string, hurricane::client_fill*> client_fills;

  unsigned long me;
  unsigned int port;
  bool connection_to_server_done;
  bool already_connect_at_least_one; // Allow to "join" a network in reconnect method when this value is false
  unsigned int first_known_lost_node;

  vector<boost::shared_ptr<TTransport>> transports;
  vector<IODFSClient*> clients;

  boost::asio::deadline_timer* reconnect_timer;
  boost::asio::io_service reconnect_thread;
  boost::asio::io_service::work* reconnect_woker;

 public:
  IODFSHandler(unsigned long _me, unsigned long _port, unsigned long _slipchunk)
  : slipchunk(_slipchunk), 
    me(_me), 
    port(_port), 
    connection_to_server_done(false),
    already_connect_at_least_one(false)  {

    // Copy all the server from hurricane.ini
    for(unsigned int i = 0; i < pt_hurricane.get < unsigned long > ("machines.count"); ++i){
      stringstream name;
      stringstream iface;
      name << "machines.name" << i;
      iface << "machines.iface" << i;
      add_new_machine_in_machines(name.str(), iface.str());
    } 

    first_known_lost_node = machines.size();

    init_req();  

    hurricane::init(&local_buffers, true);

    // Shuffling the machine in order that every machine have ~ the same amount of requests
    string me_name = machines[me].name;
    random_shuffle(machines.begin(), machines.end());
    me = machines.size()+1;
    for(unsigned int i = 0; me > machines.size() && i < machines.size(); ++i)
      if(machines[i].name == me_name)
        me = i;

    for(auto& machine : machines)
      add_new_machine_network(machine.name);

    // Necessary if we want to reuse our thread
    reconnect_woker = new boost::asio::io_service::work(reconnect_thread);

    // Do the connection later
    reconnect_timer = new boost::asio::deadline_timer(reconnect_thread, boost::posix_time::seconds(NB_SECONDS_BETWEEN_RECONNECTION));
    reconnect_timer->async_wait(boost::bind(&IODFSHandler::reconnect, this));
    hurricane::threadpool.create_thread(boost::bind(&boost::asio::io_service::run, &reconnect_thread));
  }

  ~IODFSHandler() {
    hurricane::shutdown(true);

    reconnect_thread.stop();

    for(unordered_map<string, hurricane::io*>::iterator it = local_buffers.begin(); it != local_buffers.end(); ++it)
      helper_delete(it);
  
    for(auto& it : client_fills) {
      it.second->teardown_conn();
      delete it.second;
    }

    for(auto& it : client_drains) {
      it.second->teardown_conn();
      delete it.second;
    }

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
  bool local_create(const string& filename) {
    if(local_exists(filename))
      return false;

    local_buffers[filename] = new hurricane::io(filename.c_str(), NULL);
    return true;
  }

  bool local_exists(const string& filename) {
    return helper_exists(filename, local_buffers);
  }

  // False indicates that the file already exists
  bool local_delete(const string& filename) {
    unordered_map<string, hurricane::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    helper_delete(it);
    local_buffers.erase(it);

    // Remove from global_buffer if the request comes from global
    if(global_buffers.find(filename) != global_buffers.end())
      global_buffers.erase(filename);

    return true;
  }

  // An empty buffer implies either the file already exists or we are at the EOF
  void local_fill(string& _return, const string& filename, const int64_t size) {
    unordered_map<string, hurricane::io*>::iterator it;
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
  void local_seek_and_fill(string& _return, const string& filename, const int64_t size, const int64_t offset) {
    unordered_map<string, hurricane::io*>::iterator it;
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
  bool local_drain(const string& filename, const string& buffer) {
    unordered_map<string, hurricane::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    init_req();
    req.size = buffer.size();
    it->second->drain((unsigned char*) buffer.c_str(), &req);
    return true;
  }

  // False indicate that the file doesn't exist
  bool local_rewind(const string& filename) {
    unordered_map<string, hurricane::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    it->second->rewind();
    return true;
  }

  // False indicate that the file doesn't exist
  bool local_trunc(const string& filename) {
    unordered_map<string, hurricane::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return false;

    it->second->trunc();
    return true;
  }

  // True indicate that the file doesn't exist of we are EOF
  bool local_eof(const string& filename) {
    unordered_map<string, hurricane::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return true;

    return it->second->eof();
  }

  // -1 indicate that the file doesn't exist
  int64_t local_size(const string& filename) {
    unordered_map<string, hurricane::io*>::iterator it;
    if(!helper_exists(filename, it, local_buffers))
      return -1;

    return it->second->get_size();
  }

  // -1 indicate that the file doesn't exist
  int64_t local_left(const string& filename) {
    unordered_map<string, hurricane::io*>::iterator it;
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
  // Throw LostConnection if the server is blocked (loosing a node or initializing) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  // Throw BadToken if the token is not recognized
  bool global_create(const string& filename, const string& token) {
    handle_connection_to_server_done(true);
    handle_token(token);

    if(global_exists(filename, token) || helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_create(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].name;
      throw lcdc;
    }

    global_buffers[filename] = NULL;

    return result;
  }

  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  // Throw BadToken if the token is not recognized
  bool global_exists(const string& filename, const string& token) {
    handle_connection_to_server_done(true);
    handle_token(token);

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_exists(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].name;
      throw lcdc;
    }

    return result;
  }

  // False implie that the file doesn't exist on this machine or on another machine
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  // Throw BadToken if the token is not recognized
  bool global_delete(const string& filename, const string& token) {
    handle_connection_to_server_done(true);
    handle_token(token);

    if(!global_exists(filename, token) || !helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_delete(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].name;
      throw lcdc;
    }

    return result;
  }

  // An empty buffer implies that either the file doesn't exisir or the size is not a multiple of slipchunk !
  // Throw LostConnection if the server is blocked (initializing)
  // Throw BadToken if the token is not recognized
  void global_fill(string& _return, const string& filename, const int64_t size, const string& token) {
    handle_connection_to_server_done(false);
    handle_token(token, true, false);

    if(!global_exists(filename, token) || !helper_exists(filename, global_buffers) || (size%slipchunk != 0))
      _return = "";
    else {
      init_req();
      strcpy(req.filename, filename.c_str());
      req.cmd = hurricane::CMD_FILL;
      req.size = size;

      _return = string(size, 0);
      int64_t filled_bytes = client_fills[token]->batch_access_store(&req, (unsigned char*) _return.c_str(), slipchunk, size);

      // EOF, should return a string smaller and so, its size is modified
      if(filled_bytes < size) 
          _return = _return.data();
    }
  }

  // Throw LostConnection if the server is blocked (initializing)
  // Throw BadToken if the token is not recognized
  bool global_drain(const string& filename, const string& buffer, const string& token) {
    handle_connection_to_server_done(false);
    handle_token(token, false, true);

    if(!global_exists(filename, token) || !helper_exists(filename, global_buffers))
      return false;

    init_req();
    strcpy(req.filename, filename.c_str());
    req.cmd = hurricane::CMD_DRAIN;
    req.size = buffer.size();

    if (client_drains[token]->batch_access_store(&req, (unsigned char*) buffer.c_str(), slipchunk, buffer.size())) {
      BOOST_LOG_TRIVIAL(fatal) << "Unable to write to hurricane";
      return false;
    }

    return true;
  }

  // False implie that the file doesn't exist on this machine or on another machine
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  // Throw BadToken if the token is not recognized
  bool global_rewind(const string& filename, const string& token) {
    handle_connection_to_server_done();
    handle_token(token);

    if(!global_exists(filename, token) || !helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_rewind(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].name;
      throw lcdc;
    }

    return result;
  }

  // False implie that the file doesn't exist on this machine or on another machine
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  // Throw BadToken if the token is not recognized
  bool global_trunc(const string& filename, const string& token) {
    handle_connection_to_server_done();
    handle_token(token);

    if(!global_exists(filename, token) || !helper_exists(filename, global_buffers))
      return false;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_trunc(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].name;
      throw lcdc;
    }

    return result;
  }

  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  // Throw BadToken if the token is not recognized
  bool global_eof(const string& filename, const string& token) {
    handle_connection_to_server_done();
    handle_token(token);

    if(!global_exists(filename, token) || !helper_exists(filename, global_buffers))
      return true;

    bool result = true;
    try {
      for(first_known_lost_node = 0; result && first_known_lost_node < clients.size(); ++first_known_lost_node)
        result = result && clients[first_known_lost_node]->local_eof(filename);
    } catch(TException& tx) {
      LostConnectionDuringCommit lcdc;
      lcdc.name_node_lost = machines[first_known_lost_node].name;
      throw lcdc;
    }

    return result;
  }

  // -1 indicate that on a machine, the file doesn't exist
  // Throw LostConnection if the server is blocked (loosing a node) or we lost a node during the voting phase
  // Throw LostConnectionDuringCommit if we lost a node during the commit phase, we might be in a inconsistency phase
  // Throw BadToken if the token is not recognized
  int64_t global_size(const string& filename, const string& token) {
    handle_connection_to_server_done();
    handle_token(token);

    if(!global_exists(filename, token) || !helper_exists(filename, global_buffers))
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
      lcdc.name_node_lost = machines[first_known_lost_node].name;
      throw lcdc;
    }

    return result;
  }




  ////////////////////////////////////////////
  //////////// JOIN PART    //////////////////
  ////////////////////////////////////////////



 
  void generate_token(string& _return) {
    const string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    _return = "";

    for(int i = 0; i < LENGTH_TOKEN; ++i)
      _return += characters[rand() % (characters.size() -1)];

    tokens[_return] = NULL;
  }

  // Create a connection with a client, given a generated token
  bool connect(const string& token) {
    if (tokens.find(token) == tokens.end())
      return false;

    client_drains[token] = new hurricane::client_drain();
    client_fills[token] = new hurricane::client_fill();

    return true;
  }

  // Close the connection with the client corresponding to the token. The token is erased.
  bool leave(const string& token) {
    if (tokens.find(token) == tokens.end() || client_fills.find(token) == client_fills.end() || client_drains.find(token) == client_drains.end())
      return false;

    client_drains[token]->teardown_conn();
    client_fills[token]->teardown_conn();

    delete client_drains[token];
    delete client_fills[token];

    client_drains.erase(token);
    client_fills.erase(token);
    tokens.erase(token);

    return true;
  }

  // Return the list of known servers. The new server needs to call join on each server
  void get_list_server_nodes(vector<network_interface> & _return) {
    handle_connection_to_server_done(true);

    // Return the list of servers (excluded the new one)
    _return = machines;
  }

  void join(const string& name, const string& iface) {
    std::cout << "JOIN from " << name << " " << iface << std::endl;
    add_machines.lock();
    add_new_machine(name, iface);
    add_machines.unlock();
  }




  ////////////////////////////////////////////
  //////////// PRIVATE PART //////////////////
  ////////////////////////////////////////////



  
private:
  bool helper_exists(const string& filename, unordered_map<string, hurricane::io*>::iterator& it, unordered_map<string, hurricane::io*>& buffers) {
    it = buffers.find(filename);
    return it != buffers.end();
  }

  bool helper_exists(const string& filename, unordered_map<string, hurricane::io*>& buffers) {
    return buffers.find(filename) != buffers.end();
  }

  void helper_delete(unordered_map<string, hurricane::io*>::iterator& it) {
    delete it->second;
    unlink_file(((char*)it->first.c_str()));
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
          BOOST_LOG_TRIVIAL(info) << "Connection to " << machines[i].name << " SUCCESS !";
        }
        ++i;
      }

      // To do the join, we assume that we only know ONE server (+ us)
      if(!already_connect_at_least_one && machines.size() == 2) {
        already_connect_at_least_one = true;
      
        // Find the index of the master (which musn't be "me" !)
        unsigned int master = 1-me;

        vector<network_interface> servers;
        try {
          // We assume that in the case where a client wants to join a network, it knows only 1 server
          // If the whole network isn't safe, don't join it !
          clients[master]->get_list_server_nodes(servers); 
        } catch (LostConnection& lc) {
          BOOST_LOG_TRIVIAL(fatal) << "The network isn't safe !";
          exit(-1);
        }

        // Connect to all the server node
        for(auto& machine : servers) {
          std::cout << machine.name << " " << machine.iface << std::endl;

          // Avoid to add again to a server node if already did it (or knew it when launching the server)
          if (machine.name != machines[master].name) {
            add_new_machine(machine.name, machine.iface);
            transports.back()->open();
            clients.back()->join(machine.name, machine.iface);
          }
          else
            clients[master]->join(machine.name, machine.iface); //Notify the master that he has joined the network

          BOOST_LOG_TRIVIAL(info) << "Join to " << machine.name << " DONE !";
        }

        // Now connect to all the server but without joining them again
        reconnect();
      }
      else
        connection_to_server_done = true;
    } catch (TException& tx){
      transports[i]->close();
      BOOST_LOG_TRIVIAL(info) << "Connection to " << machines[i].name << " FAILED : " << tx.what();
      reconnect_timer->expires_at(reconnect_timer->expires_at() + boost::posix_time::seconds(NB_SECONDS_BETWEEN_RECONNECTION));
      reconnect_timer->async_wait(boost::bind(&IODFSHandler::reconnect, this));
    }
  }

  void try_to_reconnect() {
    connection_to_server_done = false;
    reconnect_timer->async_wait(boost::bind(&IODFSHandler::reconnect, this));
    hurricane::threadpool.create_thread(boost::bind(&boost::asio::io_service::run, &reconnect_thread));
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
      BOOST_LOG_TRIVIAL(info) << "Connection to " << machines[first_known_lost_node].name << " FAILED : " << tx.what();
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
      lc.name_node_lost = first_known_lost_node < machines.size() ? machines[first_known_lost_node].name : "Server hasn't been initialised";
      throw lc;
    }
  }

  void handle_token(const string& token, bool check_fill = false, bool check_drain = false) {
    if (tokens.find(token) == tokens.end() || (check_fill && (client_fills.find(token) == client_fills.end()))  || (check_drain && (client_drains.find(token) == client_drains.end())) ) {
      BadToken bt;
      throw bt;
    }
  }

  void add_new_machine_in_machines(const string& key_name, const string& key_iface) {
    network_interface ni;
    ni.name = pt_hurricane.get<string>(key_name.c_str());
    ni.iface = pt_hurricane.get<string>(key_iface.c_str());

    machines.push_back(ni);
  }

  void add_new_machine_network(const string& name) {
    // Init the network part
    boost::shared_ptr<TTransport> socket(new TSocket(name.c_str(), port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    IODFSClient* client = new IODFSClient(protocol);

    transports.push_back(transport);
    clients.push_back(client);
  }

  bool add_new_machine(const string& name, const string& iface) {
    std::cout << "Adding new machine : " << name << " " << iface << std::endl;
    bool exists = false;
    for(unsigned int i = 0; !exists && i < machines.size(); ++i) {
      exists = (machines[i].name == name);
      if(exists)
        std::cout << machines[i] << std::endl;
    }
    std::cout << "value of exists : " << exists << std::endl;
    if(!exists) {
      string key_name, key_iface;
      add_new_machine_in_pt_hurricane(name, iface, key_name, key_iface);
      add_new_machine_in_machines(key_name, key_iface);
      add_new_machine_network(name);
    }

    return !exists;
  }

  void add_new_machine_in_pt_hurricane(const string& _name, const string& _iface, string& key_name, string& key_iface) {
    stringstream name;
    stringstream iface;
    size_t i = machines.size();
    name << "machines.name" << (i-1);
    iface << "machines.iface" << (i-1);
    key_name = name.str();
    key_iface = iface.str();

    pt_hurricane.put("machines.count", i+1);
    pt_hurricane.put(key_name.c_str(), _name);
    pt_hurricane.put(key_iface.c_str(), _iface);

    for(unsigned int i = 0; i < pt_hurricane.get < unsigned long > ("machines.count"); ++i){
      stringstream name;
      stringstream iface;
      name << "machines.name" << i;
      iface << "machines.iface" << i;

      std::cout << pt_hurricane.get < unsigned long > ("machines.count") << "\t" << pt_hurricane.get<string>(name.str().c_str()) << " " << pt_hurricane.get<string>(iface.str().c_str()) << std::endl;
    } 
  }

};





  ////////////////////////////////////////////
  //////////// MAIN ////// ///////////////////
  ////////////////////////////////////////////



  
int main(int argc, const char **argv) {
  int port = PORT;

  srand (time(NULL));
  setup_options(argc, argv);
  
  init_hurricane_desc();
  unsigned long me = pt_hurricane.get < unsigned long > ("machines.me");

  unsigned long slipchunk = vm["slipchunk"].as < unsigned long > ();
  unsigned long align = vm["slipbench_align"].as < unsigned long > ();
  slipchunk = (slipchunk / align) * align;

  bool batching = (vm.count("request_batching") > 0);
  unsigned long batch_size = vm["batch_size"].as < unsigned long > ();

  BOOST_LOG_TRIVIAL(info) << "BATCHING : " << batching;
  BOOST_LOG_TRIVIAL(info) << "SLIPCHUNK" << slipchunk;
  BOOST_LOG_TRIVIAL(info) << "BATCH SIZE" << batch_size;

  TThreadedServer server(
    boost::make_shared<IODFSProcessor>(boost::make_shared<IODFSHandler>(me, port, slipchunk)),
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
