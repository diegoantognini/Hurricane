#include "gen-cpp/IODFS.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <iostream>
#include <cassert>
#include <ctime>
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
	string file1 = "file1.txt";

	// Non-existing file
	result = result && !client.local_exists(file1);
	result = result && !client.local_delete(file1);

	// Creation file
	result = result && client.local_create(file1);
	result = result && !client.local_create(file1); 

	// Existence
	result = result && client.local_exists(file1);

	// Deletion file
	result = result && client.local_delete(file1);
	result = result && !client.local_delete(file1);
	result = result && !client.local_exists(file1);

	// Recreation file
	result = result && client.local_create(file1);
	result = result && client.local_exists(file1);

	result = result && client.local_delete(file1);

	return result;
}

bool test_local_create_exists_delete_two_files(IODFSClient& client) {
	bool result = true;
	string file1 = "file1.txt";
	string file2 = "file2.txt";

	// Creation file1
	result = result && client.local_create(file1);
	result = result && !client.local_create(file1); 

	// Existence
	result = result && client.local_exists(file1);
	result = result && !client.local_exists(file2);

	// Creation file2
	result = result && client.local_create(file2);
	result = result && client.local_exists(file1);
	result = result && client.local_exists(file2);

	// Deletion file1
	result = result && client.local_delete(file1);
	result = result && !client.local_delete(file1);
	result = result && !client.local_exists(file1);
	result = result && client.local_exists(file2);

	// Deletion
	return result;
}

bool test_local_drain_fill(IODFSClient& client) {
	string filename = "testing_drain";
	const unsigned long SLIPCHUNCK = 16;//4*1024*1024; // 4 Mo
	const unsigned long LENGTH = 4*SLIPCHUNCK;
	string buffer(SLIPCHUNCK, 0);
	unsigned long results = 0;
	unsigned long read_results = 0;
	bool result = true;

	result = result && client.local_create(filename);
	result = result && client.local_exists(filename);

	// Drain
	unsigned long bytes_to_write = LENGTH;
	while(bytes_to_write > 0) {
		std::cout << bytes_to_write << std::endl;
		char var = 0;
		for(unsigned long i = 0; i < SLIPCHUNCK; ++i) {
			var = rand()%127 + 1;
			results += var;

			buffer[i] = var;
			cout << buffer[i] << " ";
		}

		result = result && client.local_drain(filename, buffer);
		bytes_to_write -= buffer.size();
	}
	cout << endl;

	// Fill
	std::string read_buffer;
	unsigned long bytes_left = LENGTH;
	unsigned long bytes_to_read = 0;
	while(bytes_left > 0) {
		bytes_to_read = (bytes_left > SLIPCHUNCK ? SLIPCHUNCK : bytes_left);
		client.local_fill(read_buffer, filename, bytes_to_read);

		if(bytes_to_read != read_buffer.size())
			results = false;

		bytes_left -= bytes_to_read;

		for(unsigned int i = 0; i < bytes_to_read; ++i)
			read_results += read_buffer[i];
	}
	
	std::cout << results << " " << read_results << std::endl;

	return true;
}
void do_test(const char* name, bool (*fct)(IODFSClient&), IODFSClient& client) {
	cout << name << " : ";
	cout << " -> " << (fct(client) ? "PASSED" : "FAILED") << endl;
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

	    do_test("test_local_create_exists_delete_one_file", &test_local_create_exists_delete_one_file, iodfs);
	    do_test("test_local_create_exists_delete_two_files", &test_local_create_exists_delete_two_files, iodfs);
	    do_test("test_local_drain_fill", &test_local_drain_fill, iodfs);

		transport->close();
	} catch (TException& tx) {
		cout << "ERROR: " << tx.what() << endl;
	}

	return 0;
}
