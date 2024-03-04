SET CLUSTER SETTING kv.rangefeed.enabled = true;

CREATE TABLE IF NOT EXISTS users (
	id varchar primary key,
	name varchar,
	email varchar,
	updated_at timestamp
);

CREATE TABLE IF NOT EXISTS posts (
	id varchar primary key,
	content varchar,
	author varchar,
	updated_at timestamp
);

CREATE TABLE IF NOT EXISTS comments (
	id varchar primary key,
	content varchar,
	post varchar,
	author varchar,
	updated_at timestamp
);
