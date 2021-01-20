CREATE TABLE IF NOT EXISTS arr_test
(
    id    String,
    value Array(Float64)
) ENGINE = ReplacingMergeTree()
      ORDER BY (id);

INSERT INTO arr_test (id, value) VALUES ('1ff954bb-9808-4309-9955-fccf1a26266e', [0, 1]);
