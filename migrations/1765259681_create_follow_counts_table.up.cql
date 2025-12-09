CREATE TABLE IF NOT EXISTS follow_counts (
	did TEXT PRIMARY KEY,
	follows_count COUNTER,
	followers_count COUNTER,
);
