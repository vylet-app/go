CREATE TABLE IF NOT EXISTS follows_by_uri (
	uri TEXT PRIMARY KEY,
	cid TEXT,
	subject_did TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
);
