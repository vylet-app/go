CREATE TABLE IF NOT EXISTS likes_by_actor (
	uri TEXT,
	cid TEXT,
	subject_uri TEXT,
	subject_cid TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
	PRIMARY KEY (author_did, created_at, uri)
) WITH CLUSTERING ORDER BY (created_at DESC, uri ASC);
