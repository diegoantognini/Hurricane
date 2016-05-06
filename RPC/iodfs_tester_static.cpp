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





  ////////////////////////////////////////////
  //////////// LOCAL PART ////////////////////
  ////////////////////////////////////////////




  
bool test_local_create_exists_delete_one_file(IODFSClient& client, const std::string& token = "") {
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

bool test_local_create_exists_delete_two_files(IODFSClient& client, const std::string& token = "") {
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

bool helper_test_local_drain_fill_eof_size_trunc_left(IODFSClient& client, int64_t SLIPCHUNCK, int64_t LENGTH, const std::string& token = "") {
	string filename = "testing_local_drain_fill";
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
	int64_t bytes_to_write = LENGTH;
	while(bytes_to_write > 0) {
		char var = 0;
		for(int64_t i = 0; i < SLIPCHUNCK; ++i) {
			var = rand()%127 + 1;

			buffer[i] = var;
		}
		write_results += buffer;

		result = result && client.local_drain(filename, buffer);
		result = result && (client.local_left(filename) == 0);
		result = result && client.local_eof(filename);
		bytes_to_write -= buffer.size();
		result = result && (client.local_size(filename) == (LENGTH-bytes_to_write));
	}

	// Size
	result = result && (client.local_size(filename) == LENGTH);

	// Fill s.t. slipchunk % SLIPCHUNK != 0
	result = result && client.local_rewind(filename);

	// Size
	result = result && (client.local_size(filename) == LENGTH);

	std::string read_buffer;
	int64_t bytes_left = LENGTH;
	int64_t bytes_to_read = 0;
	int64_t slipchunck = SLIPCHUNCK/3; //We want to check if it works it 
	while(bytes_left > 0) {
		// Left
		result = result && (client.local_left(filename) == bytes_left);

		bytes_to_read = (bytes_left > slipchunck ? slipchunck : bytes_left);
		client.local_fill(read_buffer, filename, bytes_to_read);

		result = result && (bytes_to_read == ((int64_t)read_buffer.size()));
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

		result = result && (bytes_to_read == ((int64_t)read_buffer.size()));
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

bool test_local_drain_fill_eof_size_trunc_left(IODFSClient& client, const std::string& token = "") {
	string filename = "testing_local_drain_fill";
	unsigned long SLIPCHUNCK = 4*1024*1024; // 4 Mo
	unsigned long LENGTH = 8*SLIPCHUNCK;

	bool result = true;
	result = result && helper_test_local_drain_fill_eof_size_trunc_left(client, SLIPCHUNCK, LENGTH);
	SLIPCHUNCK = 712*716*812;
	LENGTH = 7*SLIPCHUNCK;
	result = result && helper_test_local_drain_fill_eof_size_trunc_left(client, SLIPCHUNCK, LENGTH);

	return result;
}

bool test_local_seek_and_fill(IODFSClient& client, const std::string& token = "") {
	string filename = "testing_local_seek_and_fill";
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





  ////////////////////////////////////////////
  //////////// GLOBAL PART ///////////////////
  ////////////////////////////////////////////




bool test_global_create_exists_delete_size_eof_one_file(IODFSClient& client, const std::string& token) {
	bool result = true;
	string file1 = "file1.txt";

	// Non-existing file
	result = result && !client.global_exists(file1, token);
	result = result && !client.global_delete(file1, token);
	result = result && (client.global_size(file1, token) == -1);
	result = result && client.global_eof(file1, token);

	// Creation file
	result = result && client.global_create(file1, token);
	result = result && !client.global_create(file1, token); 
	result = result && (client.global_size(file1, token) == 0);
	result = result && client.global_eof(file1, token);

	// Existence
	result = result && client.global_exists(file1, token);
	result = result && (client.global_size(file1, token) == 0);
	result = result && client.global_eof(file1, token);

	// Deletion file
	result = result && client.global_delete(file1, token);
	result = result && (client.global_size(file1, token) == -1);
	result = result && !client.global_delete(file1, token);
	result = result && !client.global_exists(file1, token);
	result = result && (client.global_size(file1, token) == -1);
	result = result && client.global_eof(file1, token);

	// Recreation file
	result = result && client.global_create(file1, token);
	result = result && client.global_exists(file1, token);
	result = result && (client.global_size(file1, token) == 0);
	result = result && client.global_eof(file1, token);

	result = result && client.global_delete(file1, token);
	result = result && (client.global_size(file1, token) == -1);
	result = result && client.global_eof(file1, token);

	return result;
}

bool test_global_create_exists_delete_size_eof_two_files(IODFSClient& client, const std::string& token) {
	bool result = true;
	string file1 = "file1.txt";
	string file2 = "file2.txt";

	// Creation file1
	result = result && client.global_create(file1, token);
	result = result && !client.global_create(file1, token); 
	result = result && (client.global_size(file1, token) == 0);
	result = result && (client.global_size(file2, token) == -1);
	result = result && client.global_eof(file1, token);
	result = result && client.global_eof(file2, token);

	// Existence
	result = result && client.global_exists(file1, token);
	result = result && !client.global_exists(file2, token);
	result = result && (client.global_size(file1, token) == 0);
	result = result && (client.global_size(file2, token) == -1);
	result = result && client.global_eof(file1, token);
	result = result && client.global_eof(file2, token);

	// Creation file2
	result = result && client.global_create(file2, token);
	result = result && client.global_exists(file1, token);
	result = result && client.global_exists(file2, token);
	result = result && (client.global_size(file1, token) == 0);
	result = result && (client.global_size(file2, token) == 0);
	result = result && client.global_eof(file1, token);
	result = result && client.global_eof(file2, token);

	// Deletion file1
	result = result && client.global_delete(file1, token);
	result = result && !client.global_delete(file1, token);
	result = result && !client.global_exists(file1, token);
	result = result && client.global_exists(file2, token);
	result = result && (client.global_size(file1, token) == -1);
	result = result && (client.global_size(file2, token) == 0);
	result = result && client.global_eof(file1, token);
	result = result && client.global_eof(file2, token);

	// Deletion file2
	result = result && client.global_delete(file2, token);
	result = result && !client.global_delete(file2, token);
	result = result && (client.global_size(file1, token) == -1);
	result = result && (client.global_size(file2, token) == -1);
	result = result && client.global_eof(file1, token);
	result = result && client.global_eof(file2, token);

	return result;
}

bool helper_test_global_drain_fill(IODFSClient& client, unsigned long SLIPCHUNCK, int64_t LENGTH, int64_t LENGTH2, const std::string& token) {
	string filename = "testing_global_drain_fill";
	string buffer(LENGTH, 0);
	bool result = true;

	//Basic empty tests
	result = result && !client.global_drain(filename, buffer, token);
	string dummy_buffer;
	client.global_fill(dummy_buffer, filename, LENGTH, token);
	result = result && dummy_buffer.size() == 0;
	result = result && (client.global_size(filename, token) == -1);
	result = result && client.global_eof(filename, token);
	result = result && !client.global_rewind(filename, token);

	result = result && client.global_create(filename, token);
	result = result && client.global_exists(filename, token);
	
	// Size
	result = result && (client.global_size(filename, token) == 0);

	// Drain
	char var = 0;
	for(int64_t i = 0; i < LENGTH; ++i) {
		var = rand()%127 + 1;
		buffer[i] = var;
	}

	result = result && client.global_drain(filename, buffer, token);

	usleep(5*1000*1000); // As a "cyclic barrier"

	result = result && (client.global_size(filename, token) == LENGTH);

	// Fill all the file
	result = result && client.global_rewind(filename, token);
	result = result && (client.global_size(filename, token) == LENGTH);
	
	std::string read_buffer(LENGTH2, 0);
	client.global_fill(read_buffer, filename, LENGTH2, token);

	result = result && (LENGTH == ((int64_t) read_buffer.size()));
	result = result && (client.global_size(filename, token) == LENGTH);
	result = result && client.global_eof(filename, token);

	std::sort(read_buffer.begin(), read_buffer.end());
	std::string buffer_old(buffer);
	std::sort(buffer.begin(), buffer.end());

	result = result && (read_buffer == buffer);

	// Trunc
	result = result && client.global_trunc(filename, token);

	result = result && client.global_size(filename, token) == 0;

	// Delete
	result = result && client.global_delete(filename, token);

	return result;
}

bool helper_chunk_test_global_drain_fill(IODFSClient& client, unsigned long SLIPCHUNCK, int64_t LENGTH_DRAIN, int64_t LENGTH_FILL, const std::string& token) {
	string filename = "testing_global_drain_fill";
	string buffer(LENGTH_DRAIN, 0);
	bool result = true;
	
	result = result && client.global_create(filename, token);
	result = result && client.global_exists(filename, token);

	// Drain
	char var = 0;
	for(int64_t i = 0; i < LENGTH_DRAIN; ++i) {
		var = rand()%127 + 1;
		buffer[i] = var;
	}

	result = result && client.global_drain(filename, buffer, token);

	usleep(5*1000*1000); // As a "cyclic barrier"

	result = result && (client.global_size(filename, token) == LENGTH_DRAIN);

	// Fill only a part of the file
	result = result && client.global_rewind(filename, token);
	result = result && (client.global_size(filename, token) == LENGTH_DRAIN);

	std::string res = "";
	std::string read_buffer(LENGTH_FILL, 0);
	do{
		client.global_fill(read_buffer, filename, LENGTH_FILL, token);
		res += read_buffer;

		result = result && (LENGTH_FILL == ((int64_t) read_buffer.size()));
		result = result && (client.global_size(filename, token) == LENGTH_DRAIN);
	}while(res.size() != buffer.size());

	std::sort(res.begin(), res.end());
	std::sort(buffer.begin(), buffer.end());

	result = result && (res == buffer);
	
	// Trunc
	result = result && client.global_trunc(filename, token);

	result = result && client.global_size(filename, token) == 0;

	// Delete
	result = result && client.global_delete(filename, token);

	return result;
}

bool test_global_drain_fill(IODFSClient& client, const std::string& token) {
	bool result = true;
	
	// first case, we read all the file which is a multiple of slipchunk
	int64_t SLIPCHUNCK = 4*1024*1024; // 4 Mb
	int64_t LENGTH_DRAIN = 32*SLIPCHUNCK;
	int64_t LENGTH_FILL = LENGTH_DRAIN;
	result = result && helper_test_global_drain_fill(client, SLIPCHUNCK, LENGTH_DRAIN, LENGTH_FILL, token);
	
	// second case : we write an non-multiple of slipchunk and want to read the whole file (the length must be a multiple of slipchunk, but the buffer will be smaller at the end)	
	LENGTH_DRAIN = 133*1024*1024;
	LENGTH_FILL = 140*1024*1024; //Read more than our 135Mb
	result = result && helper_test_global_drain_fill(client, SLIPCHUNCK, LENGTH_DRAIN, LENGTH_FILL, token);
	
	// third case : same as first but only a chunk
	LENGTH_DRAIN = 32*SLIPCHUNCK;
	LENGTH_FILL = 4*SLIPCHUNCK;
	result = result && helper_chunk_test_global_drain_fill(client, SLIPCHUNCK, LENGTH_DRAIN, LENGTH_FILL, token);
	
	return result;
}

/* Doesn't handle more than 1 client at a time on a given server, due to port assignments
bool test_global_token(IODFSClient& client, const std::string& not_used_token) {
	bool result = true;
	std::vector<std::string> tokens;

	for(int i = 0; i < 3; ++i){
		std::string t;
		client.generate_token(t);
		tokens.push_back(t);
		result = result && client.connect(t);
		result = result && !client.connect(t);
		result = result && !client.connect("fakeToken");
	}

	std::string filename1 = "filetest1";
	std::string filename2 = "filetest2";
		
	for(auto& token : tokens) {
		result = result && client.global_create(filename1, token);
		try {
			client.global_create(filename2, "fakeToken");
			result = result && false;
		} catch (BadToken& bd) {
			result = result && true;
		}

		result = result && client.global_exists(filename1, token);
		try {
			client.global_exists(filename2, "fakeToken");
			result = result && false;
		} catch (BadToken& bd) {
			result = result && true;
		}

		std::string buffer = "1";
		result = result && client.global_drain(filename1, buffer, token);
		try {
			client.global_drain(filename2, buffer, "fakeToken");
			result = result && false;
		} catch (BadToken& bd) {
			result = result && (buffer == "");
		}

		buffer = "";
		client.global_fill(buffer, filename1, 0, token);
		result = result && (buffer == "1");
		try {
			client.global_fill(buffer, filename2, 1, "fakeToken");
			result = result && false;
		} catch (BadToken& bd) {
			result = result && (buffer == "");
		}

		result = result && client.global_rewind(filename1, token);
		try {
			client.global_rewind(filename2, "fakeToken");
			result = result && false;
		} catch (BadToken& bd) {
			result = result && true;
		}

		result = result && client.global_trunc(filename1, token);
		try {
			client.global_trunc(filename2, "fakeToken");
			result = result && false;
		} catch (BadToken& bd) {
			result = result && true;
		}

		result = result && !client.global_eof(filename1, token);
		try {
			client.global_eof(filename2, "fakeToken");
			result = result && false;
		} catch (BadToken& bd) {
			result = result && true;
		}

		result = result && (client.global_size(filename1, token) == 1);
		try {
			client.global_size(filename2, "fakeToken");
			result = result && false;
		} catch (BadToken& bd) {
			result = result && true;
		}
	}

	for(auto& token : tokens) {
		result = result && client.leave(token);
		result = result && !client.leave(token);
		result = result && !client.leave("fakeToken");
	}

	return result;
}*/

void do_test(const char* name, bool (*fct)(IODFSClient&, const std::string&), IODFSClient& client, const std::string& token = "") {
	cout << name << " : ";
	cout << " -> " << (fct(client, token) ? "PASSED" : "FAILED") << endl;
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

void testing_global_requests(IODFSClient& client) {
	try {
		std::string token = "";
		client.generate_token(token);
		client.connect(token);
	    do_test("test_global_create_exists_delete_size_eof_one_file", &test_global_create_exists_delete_size_eof_one_file, client, token);
	    do_test("test_global_create_exists_delete_size_eof_two_files", &test_global_create_exists_delete_size_eof_two_files, client, token);
	    do_test("test_global_drain_fill", &test_global_drain_fill, client, token);
	    //do_test("test_global_token", &test_global_token, client, token); Doesn't handle more than 1 client at a time on a given server, due to port assignments
	    client.leave(token);
	} catch (TException& tx) {
		cout << "Global Error: " << tx.what() << endl;
	}
}

int main(int argc, char **argv) {
	const char* server = "128.178.52.205"; //-10g on DCO
	int port = 9090;

	srand (time(NULL));
	boost::shared_ptr<TTransport> socket(new TSocket(server, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	IODFSClient client(protocol);

	transport->open();

	testing_local_requests(client);
	testing_global_requests(client);

	transport->close();

	return 0;
}
