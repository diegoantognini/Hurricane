exception LostConnection {
}

service IODFS {
	// Local IO
	bool local_create(1:string filename),
	bool local_exists(1:string filename),
	bool local_delete(1:string filename),

	string local_fill(1:string filename, 2:i64 size),
	string local_seek_and_fill(1:string filename, 2:i64 size, 3:i64 offset),
	bool local_drain(1:string filename, 2:binary buffer),

	bool local_rewind(1:string filename),
	bool local_trunc(1:string filename),

	bool local_eof(1:string filename),
	i64 local_size(1:string filename),
	i64 local_left(1:string filename),

	// Global IO
	bool global_create(1:string filename) throws (1:LostConnection lc),
	bool global_exists(1:string filename) throws (1:LostConnection lc),
	bool global_delete(1:string filename) throws (1:LostConnection lc),

	string global_fill(1:string filename, 2:i64 size) throws (1:LostConnection lc),
	string global_seek_and_fill(1:string filename, 2:i64 size, 3:i64 offset) throws (1:LostConnection lc),
	bool global_drain(1:string filename, 2:binary buffer) throws (1:LostConnection lc),

	bool global_rewind(1:string filename) throws (1:LostConnection lc),
	bool global_trunc(1:string filename) throws (1:LostConnection lc),

	bool global_eof(1:string filename) throws (1:LostConnection lc),
	i64 global_size(1:string filename) throws (1:LostConnection lc),

	bool nop()
}