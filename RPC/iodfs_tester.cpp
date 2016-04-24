#include "gen-cpp/IODFS.h"
#include "gen-cpp/IODFS_types.h"
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

	// Deletion file2
	result = result && client.local_delete(file2);
	result = result && !client.local_delete(file2);

	return result;
}

bool test_local_drain_fill_eof_size_trunc_left(IODFSClient& client) {
	string filename = "testing_drain_fill";
	const unsigned long SLIPCHUNCK = 4*1024*1024; // 4 Mo
	const unsigned long LENGTH = 8*SLIPCHUNCK;
	string buffer(SLIPCHUNCK, 0);
	string write_results;
	string read_results;
	bool result = true;

	result = result && !client.local_drain(filename, buffer);
	string dummy_buffer;
	client.local_fill(dummy_buffer, filename, SLIPCHUNCK);
	result = result && dummy_buffer.size() == 0;
	result = result && (client.local_size(filename) == -1);
	result = result && (client.local_left(filename) == -1);
	result = result && client.local_eof(filename);
	result = result && !client.local_rewind(filename);

	result = result && client.local_create(filename);
	result = result && client.local_exists(filename);
	
	// Size
	result = result && (client.local_size(filename) == 0);

	// Drain
	unsigned long bytes_to_write = LENGTH;
	while(bytes_to_write > 0) {
		char var = 0;
		for(unsigned long i = 0; i < SLIPCHUNCK; ++i) {
			var = rand()%127 + 1;

			buffer[i] = var;
		}
		write_results += buffer;

		result = result && client.local_drain(filename, buffer);
		result = result && (client.local_left(filename) == 0);
		result = result && client.local_eof(filename);
		bytes_to_write -= buffer.size();
		result = result && (((unsigned long)client.local_size(filename)) == (LENGTH-bytes_to_write));
	}

	// Size
	result = result && (client.local_size(filename) == LENGTH);

	// Fill s.t. slipchunk % SLIPCHUNK != 0
	result = result && client.local_rewind(filename);

	// Size
	result = result && (client.local_size(filename) == LENGTH);

	std::string read_buffer;
	unsigned long bytes_left = LENGTH;
	unsigned long bytes_to_read = 0;
	unsigned long slipchunck = SLIPCHUNCK/3; //We want to check if it works it 
	while(bytes_left > 0) {
		// Left
		result = result && (((unsigned long)client.local_left(filename)) == bytes_left);

		bytes_to_read = (bytes_left > slipchunck ? slipchunck : bytes_left);
		client.local_fill(read_buffer, filename, bytes_to_read);

		result = result && (bytes_to_read == read_buffer.size());
		result = result && (client.local_size(filename) == LENGTH);

		bytes_left -= bytes_to_read;

		// EOF
		if(bytes_left > 0)
			result = result && !client.local_eof(filename);
		else
			result = result && client.local_eof(filename);
	
		read_results += read_buffer;
	}

	result = result && (read_results == write_results);

	// Fill s.t. slipchunk % SLIPCHUNK == 0
	result = result && client.local_rewind(filename);
	read_results ="";
	bytes_left = LENGTH;
	bytes_to_read = 0;
	slipchunck = SLIPCHUNCK; 
	while(bytes_left > 0) {
		bytes_to_read = (bytes_left > slipchunck ? slipchunck : bytes_left);
		client.local_fill(read_buffer, filename, bytes_to_read);

		result = result && (bytes_to_read == read_buffer.size());
		result = result && (client.local_size(filename) == LENGTH);

		bytes_left -= bytes_to_read;

		read_results += read_buffer;
	}

	result = result && (read_results == write_results);

	// Trunc
	result = result && client.local_trunc(filename);
	result = result && client.local_size(filename) == 0;
	result = result && client.local_left(filename) == 0;

	// Delete
	result = result && client.local_delete(filename);

	return result;
}

bool test_local_seek_and_fill(IODFSClient& client) {
	string filename = "testing_seek_and_fill";
	const unsigned long SLIPCHUNCK = 4*1024; // 4 Ko
	const unsigned long LENGTH = 32*SLIPCHUNCK;
	string buffer(SLIPCHUNCK, 0);
	string write_results;
	string read_results;
	bool result = true;

	result = result && !client.local_drain(filename, buffer);
	string dummy_buffer;
	client.local_seek_and_fill(dummy_buffer, filename, SLIPCHUNCK, 0);
	result = result && dummy_buffer.size() == 0;
	result = result && (client.local_size(filename) == -1);
	result = result && (client.local_left(filename) == -1);
	result = result && client.local_eof(filename);
	result = result && !client.local_rewind(filename);

	result = result && client.local_create(filename);
	result = result && client.local_exists(filename);
	
	// Size
	result = result && (client.local_size(filename) == 0);

	// Drain
	unsigned long bytes_to_write = LENGTH;
	while(bytes_to_write > 0) {
		char var = 0;
		for(unsigned long i = 0; i < SLIPCHUNCK; ++i) {
			var = rand()%127 + 1;

			buffer[i] = var;
		}
		write_results += buffer;

		result = result && client.local_drain(filename, buffer);
		bytes_to_write -= buffer.size();
	}

	// Size
	result = result && (client.local_size(filename) == LENGTH);

	// Seek and Fill reading per slipchunk
	std::string read_buffer;
	unsigned long bytes_left = LENGTH;
	unsigned long bytes_to_read = 0;
	unsigned long slipchunck = SLIPCHUNCK;
	while(bytes_left > 0) {
		bytes_to_read = (bytes_left > slipchunck ? slipchunck : bytes_left);
		client.local_seek_and_fill(read_buffer, filename, bytes_to_read, LENGTH-bytes_left);

		result = result && (bytes_to_read == read_buffer.size());

		bytes_left -= bytes_to_read;
		read_results += read_buffer;
	}

	result = result && (read_results == write_results);

	// Seek and Fill reading every byte with slipchunk
	read_buffer ="";
	bytes_left = LENGTH;
	bytes_to_read = 0;
	slipchunck = SLIPCHUNCK;
	int64_t offset = 0;
	string chunk_write_results;
	while(bytes_left > 0) {
		bytes_to_read = (bytes_left > slipchunck ? slipchunck : bytes_left);
		client.local_seek_and_fill(read_buffer, filename, bytes_to_read, offset);

		result = result && (bytes_to_read == read_buffer.size());
		result = result && (client.local_size(filename) == LENGTH);

		chunk_write_results = string(write_results.begin()+offset, write_results.begin()+offset+bytes_to_read);
		result = result && (read_buffer == chunk_write_results);

		--bytes_left;
		++offset;
	}

	// Trunc
	result = result && client.local_trunc(filename);
	result = result && client.local_size(filename) == 0;
	result = result && client.local_left(filename) == 0;

	// Delete
	result = result && client.local_delete(filename);

	return result;
}

bool test_global_create_exists_delete_size_eof_one_file(IODFSClient& client) {
	bool result = true;
	string file1 = "file1.txt";

	// Non-existing file
	result = result && !client.global_exists(file1);
	result = result && !client.global_delete(file1);
	result = result && (client.global_size(file1) == -1);
	result = result && client.global_eof(file1);

	// Creation file
	result = result && client.global_create(file1);
	result = result && !client.global_create(file1); 
	result = result && (client.global_size(file1) == 0);
	result = result && client.global_eof(file1);

	// Existence
	result = result && client.global_exists(file1);
	result = result && (client.global_size(file1) == 0);
	result = result && client.global_eof(file1);

	// Deletion file
	result = result && client.global_delete(file1);
	result = result && (client.global_size(file1) == -1);
	result = result && !client.global_delete(file1);
	result = result && !client.global_exists(file1);
	result = result && (client.global_size(file1) == -1);
	result = result && client.global_eof(file1);

	// Recreation file
	result = result && client.global_create(file1);
	result = result && client.global_exists(file1);
	result = result && (client.global_size(file1) == 0);
	result = result && client.global_eof(file1);

	result = result && client.global_delete(file1);
	result = result && (client.global_size(file1) == -1);
	result = result && client.global_eof(file1);

	return result;
}

bool test_global_create_exists_delete_size_eof_two_files(IODFSClient& client) {
	bool result = true;
	string file1 = "file1.txt";
	string file2 = "file2.txt";

	// Creation file1
	result = result && client.global_create(file1);
	result = result && !client.global_create(file1); 
	result = result && (client.global_size(file1) == 0);
	result = result && (client.global_size(file2) == -1);
	result = result && client.global_eof(file1);
	result = result && client.global_eof(file2);

	// Existence
	result = result && client.global_exists(file1);
	result = result && !client.global_exists(file2);
	result = result && (client.global_size(file1) == 0);
	result = result && (client.global_size(file2) == -1);
	result = result && client.global_eof(file1);
	result = result && client.global_eof(file2);

	// Creation file2
	result = result && client.global_create(file2);
	result = result && client.global_exists(file1);
	result = result && client.global_exists(file2);
	result = result && (client.global_size(file1) == 0);
	result = result && (client.global_size(file2) == 0);
	result = result && client.global_eof(file1);
	result = result && client.global_eof(file2);

	// Deletion file1
	result = result && client.global_delete(file1);
	result = result && !client.global_delete(file1);
	result = result && !client.global_exists(file1);
	result = result && client.global_exists(file2);
	result = result && (client.global_size(file1) == -1);
	result = result && (client.global_size(file2) == 0);
	result = result && client.global_eof(file1);
	result = result && client.global_eof(file2);

	// Deletion file2
	result = result && client.global_delete(file2);
	result = result && !client.global_delete(file2);
	result = result && (client.global_size(file1) == -1);
	result = result && (client.global_size(file2) == -1);
	result = result && client.global_eof(file1);
	result = result && client.global_eof(file2);

	return result;
}

bool test_global_drain_fill(IODFSClient& client) {
	string filename = "testing_drain_fill";
	const unsigned long SLIPCHUNCK = 4*1024; // 4 Ko
	const unsigned long LENGTH = 32*SLIPCHUNCK;
	string buffer(SLIPCHUNCK, 0);
	string write_results;
	string read_results;
	bool result = true;

	result = result && !client.global_drain(filename, buffer);
	string dummy_buffer;
	client.global_fill(dummy_buffer, filename, SLIPCHUNCK);
	result = result && dummy_buffer.size() == 0;
	result = result && (client.global_size(filename) == -1);
	result = result && client.global_eof(filename);
	result = result && !client.global_rewind(filename);

	result = result && client.global_create(filename);
	result = result && client.global_exists(filename);
	
	// Size
	result = result && (client.global_size(filename) == 0);

	// Drain
	unsigned long bytes_to_write = LENGTH;
	while(bytes_to_write > 0) {
		char var = 0;
		for(unsigned long i = 0; i < SLIPCHUNCK; ++i) {
			var = rand()%127 + 1;

			buffer[i] = var;
		}
		write_results += buffer;

		result = result && client.global_drain(filename, buffer);
		bytes_to_write -= buffer.size();
	}

	// Size
	result = result && (client.global_size(filename) == LENGTH);

	// Trunc
	result = result && client.global_trunc(filename);
	result = result && client.global_size(filename) == 0;

	// Delete
	result = result && client.global_delete(filename);

	return result;
}

void do_test(const char* name, bool (*fct)(IODFSClient&), IODFSClient& client) {
	cout << name << " : ";
	cout << " -> " << (fct(client) ? "PASSED" : "FAILED") << endl;
}

void testing_local_requests(IODFSClient& client) {
	try {
	    do_test("test_local_create_exists_delete_one_file", &test_local_create_exists_delete_one_file, client);
	    do_test("test_local_create_exists_delete_two_files", &test_local_create_exists_delete_two_files, client);
	    do_test("test_local_drain_fill_eof_size_trunc_left", &test_local_drain_fill_eof_size_trunc_left, client);
	    do_test("test_local_seek_and_fill", &test_local_seek_and_fill, client);
	} catch (TException& tx) {
		cout << "Local Error: " << tx.what() << endl;
	}
}

void testing_deconnection_reconnection(IODFSClient& client) {
	try {
	    bool result = false;
	    while(!result) {
		    try {
		    	result = client.global_create("test");
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
		result = client.global_delete("test");
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}
}

void testing_global_requests(IODFSClient& client) {
	try {
	    do_test("test_global_create_exists_delete_size_eof_one_file", &test_global_create_exists_delete_size_eof_one_file, client);
	    do_test("test_global_create_exists_delete_size_eof_two_files", &test_global_create_exists_delete_size_eof_two_files, client);
	    do_test("test_global_drain_fill", &test_global_drain_fill, client);
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

	testing_local_requests(client);
	testing_deconnection_reconnection(client);
	testing_global_requests(client);

	transport->close();

	return 0;
}
