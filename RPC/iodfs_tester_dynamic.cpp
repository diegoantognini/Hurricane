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

void do_test(const char* name, bool (*fct)(IODFSClient&, const std::string&), IODFSClient& client, const std::string& token = "") {
	cout << name << " : ";
	cout << " -> " << (fct(client, token) ? "PASSED" : "FAILED") << endl;
}

bool testing_deconnection_reconnection(IODFSClient& client, const std::string& token) {
	bool result = false;

	try {    
	    while(!result) {
		    try {
		    	result = client.global_create("test", token);
		    	//You should deconnect a machine right know
		    	int a = 0;
		    	for(int i = 0; i < 1000000000; ++i) {
		    		a+=i;
		    	}
		    } catch (LostConnection& lc) {
		    	cout << "Lost connection with : " << lc.name_node_lost << " !" << endl;
		    } catch (LostConnectionDuringCommit& lcdc) {
		    	cout << "INCONSISTENCE STATE : Lost connection with : " << lcdc.name_node_lost << " DURING COMMIT PHASE!" << endl;
		    }
		}
		result = client.global_delete("test", token);
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}

	return result;
}

void testing(IODFSClient& client, int test_case) {
	usleep(30*1000*1000);
	try {
		std::string token = "";
		client.generate_token(token);
		client.connect(token);

		switch (test_case) {
	    	case 1:
	    		do_test("testing_deconnection_reconnection", &testing_deconnection_reconnection, client, token);
	    	break;
	    }
	    client.leave(token);
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
	int test_case = 1;
	testing(client, test_case);

	transport->close();

	return 0;
}
