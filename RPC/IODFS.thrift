exception LostConnection {
	1:string name_node_lost
}

exception LostConnectionDuringCommit {
	1:string name_node_lost
}

exception BadToken {
}

struct network_interface {
	1: string name,
	2: string iface,
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

	// Join and token
	string generate_token(),
	bool connect(1:string token),
	bool leave(1:string token),
	list<network_interface> join(1:string name, 2:string iface),
	void add_node(1:string name, 2:string iface),
	
	// Global IO
	bool global_create(1:string filename, 2:string token) throws (1:LostConnection lc, 2:LostConnectionDuringCommit lcdc, 3:BadToken bt),
	bool global_exists(1:string filename, 2:string token) throws (1:LostConnection lc, 2:LostConnectionDuringCommit lcdc, 3:BadToken bt),
	bool global_delete(1:string filename, 2:string token) throws (1:LostConnection lc, 2:LostConnectionDuringCommit lcdc, 3:BadToken bt),

	string global_fill(1:string filename, 2:i64 size, 3:string token) throws (1:LostConnection lc, 2:BadToken bt),
	bool global_drain(1:string filename, 2:binary buffer, 3:string token) throws (1:LostConnection lc, 2:BadToken bt),

	bool global_rewind(1:string filename, 2:string token) throws (1:LostConnection lc, 2:LostConnectionDuringCommit lcdc, 3:BadToken bt),
	bool global_trunc(1:string filename, 2:string token) throws (1:LostConnection lc, 2:LostConnectionDuringCommit lcdc, 3:BadToken bt),

	bool global_eof(1:string filename, 2:string token) throws (1:LostConnection lc, 2:LostConnectionDuringCommit lcdc, 3:BadToken bt),
	i64 global_size(1:string filename, 2:string token) throws (1:LostConnection lc, 2:LostConnectionDuringCommit lcdc, 3:BadToken bt),

	bool nop()
}