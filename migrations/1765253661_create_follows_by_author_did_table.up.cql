CREATE TABLE IF NOT EXISTS follows_by_author_did (
	uri TEXT,
	cid TEXT,
	subject_did TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
	PRIMARY KEY (author_did, created_at, uri)
) WITH CLUSTERING ORDER BY (created_at DESC, uri ASC);
