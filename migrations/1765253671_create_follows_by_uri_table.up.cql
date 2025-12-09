CREATE TABLE IF NOT EXISTS follows_by_subject_did (
	uri TEXT PRIMARY KEY,
	cid TEXT,
	subject_did TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
);
