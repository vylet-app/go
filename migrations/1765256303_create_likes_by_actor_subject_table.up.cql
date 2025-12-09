CREATE TABLE IF NOT EXISTS likes_by_actor_subject (
    uri TEXT,
    cid TEXT,
    subject_uri TEXT,
    subject_cid TEXT,
    author_did TEXT,
    created_at TIMESTAMP,
    indexed_at TIMESTAMP,
    PRIMARY KEY ((author_did, subject_uri))
);
