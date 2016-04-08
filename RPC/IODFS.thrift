service IODFS {
	// Local IO
	bool local_create(1:string filename),
	bool local_exists(1:string filename),
	bool local_delete(1:string filename),

	string local_fill(1:string filename),
	string local_seek_and_fill(1:string filename, 2:i64 offset),
	bool local_drain(1:string filename, 2:binary buffer),

	bool local_rewind(1:string filename),
	bool local_trunc(1:string filename),

	bool local_eof(1:string filename),
	i64 local_size(1:string filename),
	i64 local_left(1:string filename),

	// Global IO
	bool global_create(1:string filename),
	bool global_exists(1:string filename),
	bool global_delete(1:string filename),

	string global_fill(1:string filename,),
	string global_seek_and_fill(1:string filename, 2:i64 offset),
	bool global_drain(1:string filename, 2:binary buffer),

	bool global_rewind(1:string filename),
	bool global_trunc(1:string filename),

	bool global_eof(1:string filename),
	i64 global_size(1:string filename),
}