#include "gen-cpp/IODFS.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <iostream>
#include <cassert>
#include <ctime>
#include <cstdlib>
#include <string>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using boost::shared_ptr;
using std::cout;
using std::endl;
using std::string;

bool test_local_create_exists_delete_one_file(IODFSClient& client) {
	bool result = true;
	const char* file1 = "file1.txt";

	// Non-existing file
	result &= !client.local_exists(file1);
	result &= !client.local_delete(file1);

	// Creation file
	result &= client.local_create(file1);
	result &= !client.local_create(file1); 

	// Existence
	result &= client.local_exists(file1);

	// Deletion file
	result &= client.local_delete(file1);
	result &= !client.local_delete(file1);
	result &= !client.local_exists(file1);

	// Recreation file
	result &= client.local_create(file1);
	result &= client.local_exists(file1);

	result &= client.local_delete(file1);
	return result;
}

bool test_local_create_exists_delete_two_files(IODFSClient& client) {
	bool result = true;
	const char* file1 = "file1.txt";
	const char* file2 = "file2.txt";

	// Creation file1
	result &= client.local_create(file1);
	result &= !client.local_create(file1); 

	// Existence
	result &= client.local_exists(file1);
	result &= !client.local_exists(file2);

	// Creation file2
	result &= client.local_create(file2);
	result &= client.local_exists(file1);
	result &= client.local_exists(file2);

	// Deletion file1
	result &= client.local_delete(file1);
	result &= !client.local_delete(file1);
	result &= !client.local_exists(file1);
	result &= client.local_exists(file2);

	// Deletion
	return result;
}

bool test_local_drain_fill(IODFSClient& client) {
	const char* filename = "testing_drain";
	const int LENGTH = 1024; //1 Ko
	string buffer(LENGTH, 0);
	unsigned long results = 0;
	bool result = true;

	int var = 0;
	for(int i = 0; i < LENGTH; ++i) {
		var = rand()%LENGTH + 1;
		results += var;

		buffer[i] = var;
	}

	result &= client.local_create(filename);
	result &= client.local_exists(filename);
	result &= client.local_drain(filename, buffer);

	return true;
}
void do_test(const char* name, bool (*fct)(IODFSClient&), IODFSClient& client) {
	cout << name;
	assert(fct(client));
	cout << " -> PASSED" << endl;
}

int main(int argc, char **argv) {
	const char* server = "localhost";
	int port = 9090;

	srand (time(NULL));
	boost::shared_ptr<TTransport> socket(new TSocket(server, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	IODFSClient iodfs(protocol);

	try {
	    transport->open();

	    do_test("test_local_create_exists_delete_one_file", &test_local_create_exists_delete_two_files, iodfs);
	    do_test("test_local_create_exists_delete_two_files", &test_local_create_exists_delete_two_files, iodfs);
	    do_test("test_local_drain_fill", &test_local_drain_fill, iodfs);

		transport->close();
	} catch (TException& tx) {
		cout << "ERROR: " << tx.what() << endl;
	}

	return 0;
}
