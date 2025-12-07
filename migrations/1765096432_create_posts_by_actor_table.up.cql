CREATE TABLE IF NOT EXISTS posts_by_actor (
	uri TEXT,
	cid TEXT,
	author_did TEXT,
	caption TEXT,
	facets TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
	PRIMARY KEY (author_did, created_at, uri),
) WITH CLUSTERING ORDER BY (created_at DESC, uri ASC);
