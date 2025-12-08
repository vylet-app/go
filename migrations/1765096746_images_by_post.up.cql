CREATE TABLE IF NOT EXISTS images_by_post (
	post_uri TEXT,
	image_index INT,
	cid TEXT,
	alt TEXT,
	width INT,
	height INT,
	size INT,
	mime TEXT,
	PRIMARY KEY (post_uri, image_index),
) WITH CLUSTERING ORDER BY (image_index ASC);
