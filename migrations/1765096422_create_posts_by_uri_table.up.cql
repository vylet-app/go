CREATE TABLE IF NOT EXISTS posts_by_uri (
	uri TEXT PRIMARY KEY,
	cid TEXT,
	author_did TEXT,
	caption TEXT,
	facets TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
)
