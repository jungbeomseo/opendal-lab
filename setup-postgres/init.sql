--- create a table
--- ref: https://github.com/apache/opendal/pull/2815
CREATE TABLE data(
    key TEXT PRIMARY KEY,
    value BYTEA
);