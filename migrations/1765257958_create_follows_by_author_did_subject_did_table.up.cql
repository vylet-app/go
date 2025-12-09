CREATE TABLE IF NOT EXISTS follows_by_author_did_subject_did (
	uri TEXT,
	cid TEXT,
	subject_did TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
	PRIMARY KEY ((author_did, subject_did))
);
