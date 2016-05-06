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

bool testing_join1(IODFSClient& client) {
	bool result = true;

	try {    
		std::string token = "";
	    client.generate_token(token);
		result = result && client.connect(token);

		for(int i = 0; i < 10; ++i) {
			std::string filename = "file_4_" + std::to_string(i);
			result = result && client.global_create(filename, token);
			usleep(60*1000*1000);
		}

		for(int i = 0; i < 10; ++i) {
			std::string filename = "file_4_" + std::to_string(i);
			result = result && client.global_delete(filename, token);
		}
		
		result = result && client.leave(token);		
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}

	return result;
}

bool testing_join2(IODFSClient& client) {
	bool result = true;

	try {    
		std::string token = "";
	    client.generate_token(token);
		result = result && client.connect(token);

		for(int i = 0; i < 10; ++i) {
			std::string filename = "file_5_" + std::to_string(i);
			result = result && client.global_create(filename, token);
			usleep(20*1000*1000);
		}

		for(int i = 0; i < 10; ++i) {
			std::string filename = "file_5_" + std::to_string(i);
			result = result && client.global_delete(filename, token);
		}
		
		result = result && client.leave(token);		
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}

	return result;
}

void testing(IODFSClient& client, int test_case) {
	cout << "Waiting for the test" << endl;
	usleep(TIME_BEFORE_RUNNING_TEST);
	cout << "Run the test" << endl;
	try {
		switch (test_case) {
	    	case 1:
	    		do_test("testing_deconnection_reconnection", &testing_deconnection_reconnection, client);
	    		break;
	    	case 2:
	    		do_test("testing_join1", &testing_join1, client);
	    		break;
	    	case 3:
	    		do_test("testing_join2", &testing_join2, client);
	    		break;
	    }
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}
}

int main(int argc, char **argv) {
	const char* server = "128.178.52.205"; // To be changed, server only support 1 client  //-10g on DCO
	int port = 9090;

	srand (time(NULL));
	boost::shared_ptr<TTransport> socket(new TSocket(server, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	IODFSClient client(protocol);

	transport->open();
	// Each time, you have TIME_BEFORE_RUNNING_TEST before running the test
	// Case 1 : When the servers have been initialized, deconnect a node and reconnect it a few moment later. It should work again
	// Case 2 : Run a client which connects on a network composed of n nodes (e.g. 4)
	// Case 3 : Run a client which connects on a network composed of n+1 nodes (a new node network has joint)
	int test_case = 3;
	testing(client, test_case);

	transport->close();

	return 0;
}
