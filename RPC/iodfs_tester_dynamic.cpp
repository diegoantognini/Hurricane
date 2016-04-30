#include "gen-cpp/IODFS.h"
#include "gen-cpp/IODFS_types.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <iostream>
#include <cassert>
#include <ctime>
#include <string>
#include <unistd.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using boost::shared_ptr;
using std::cout;
using std::endl;
using std::string;

#define TIME_BEFORE_RUNNING_TEST 30*1000*1000

void do_test(const char* name, bool (*fct)(IODFSClient&), IODFSClient& client) {
	cout << name << " : ";
	cout << " -> " << (fct(client) ? "PASSED" : "FAILED") << endl;
}

bool testing_deconnection_reconnection(IODFSClient& client) {
	bool result = false;

	try {    
		std::string token = "";
	    client.generate_token(token);
		client.connect(token);

	    while(!result) {
		    try {
		    	result = client.global_create("test", token);
		    	//You should deconnect a machine right know
		    	usleep(1*1000*1000);
		    } catch (LostConnection& lc) {
		    	cout << "Lost connection with : " << lc.name_node_lost << " !" << endl;
		    } catch (LostConnectionDuringCommit& lcdc) {
		    	cout << "INCONSISTENCE STATE : Lost connection with : " << lcdc.name_node_lost << " DURING COMMIT PHASE!" << endl;
		    }
		}
		result = client.global_delete("test", token);
		client.leave(token);
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}

	return result;
}

bool testing_join(IODFSClient& client) {
	bool result = true;

	try {    
		std::string token_before = "";
	    client.generate_token(token_before);
		result = result && client.connect(token_before);

		std::vector<network_interface> servers_before;
		client.get_list_server_nodes(servers_before);
		std::vector<network_interface> servers_after = servers_before;

		result = result && client.global_create("file_before", token_before);
		while(servers_before == servers_after) {
			client.get_list_server_nodes(servers_after);
			usleep(1*1000*1000);
		}

		std::string token_after = "";
	    client.generate_token(token_after);
		result = result && client.connect(token_after);

		client.global_create("file_after", token_after);

		usleep(120*1000*1000); 
		// Should check manually that file_before is ONLY in the subset of servers at te begining 
		// and file_after is in the subset of servers at the beginning + the new server which has joined

		result = result && client.global_delete("file_after", token_after);
		result = result && client.global_delete("file_before", token_before);

		result = result && client.leave(token_before);
		result = result && client.leave(token_after);		
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}

	return result;
}

void testing(IODFSClient& client, int test_case) {
	usleep(30*1000*1000);
	try {
		switch (test_case) {
	    	case 1:
	    		do_test("testing_deconnection_reconnection", &testing_deconnection_reconnection, client);
	    	break;
	    }
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}
}

int main(int argc, char **argv) {
	const char* server = "128.178.52.205";
	int port = 9090;

	srand (time(NULL));
	boost::shared_ptr<TTransport> socket(new TSocket(server, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	IODFSClient client(protocol);

	transport->open();
	// Each time, you have TIME_BEFORE_RUNNING_TEST before running the test
	// Case 1 : When the servers have been initialized, deconnect a node and reconnect it a few moment later. It should work again
	// Case 2 : Run a subset of all the servers (s.t. @labos_4) and then, use another machine to join the network (fifth server for example, joining the first server)
	int test_case = 2;
	testing(client, test_case);

	transport->close();

	return 0;
}
