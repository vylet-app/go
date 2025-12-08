CREATE TABLE IF NOT EXISTS blob_refs (
    did TEXT,
    cid TEXT,
    first_seen_at TIMESTAMP,
    processed_at TIMESTAMP,
    updated_at TIMESTAMP,
    taken_down BOOLEAN,
    takedown_reason TEXT,
    taken_down_at TIMESTAMP,
    tags SET<TEXT>,
    PRIMARY KEY (did, cid)
);
