-- MySQL initialization script

CREATE TABLE binary_data_type
(
  id         INT NOT NULL PRIMARY KEY,
  binary_col BINARY(16) NOT NULL
);

-- Insert test data with a 16-byte binary value
INSERT INTO binary_data_type (id, binary_col)
VALUES (1, UNHEX('0123456789ABCDEF0123456789ABCDEF'));
