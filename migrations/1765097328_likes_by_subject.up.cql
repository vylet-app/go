CREATE TABLE IF NOT EXISTS likes_by_subject (
	uri TEXT,
	cid TEXT,
	subject_uri TEXT,
	subject_cid TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
	PRIMARY KEY (subject_uri, created_at, uri)
) WITH CLUSTERING ORDER BY (created_at DESC, uri ASC);
