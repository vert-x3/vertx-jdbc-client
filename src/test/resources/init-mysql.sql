-- MySQL initialization script

CREATE TABLE binary_data_type
(
  id         INT NOT NULL PRIMARY KEY,
  binary_col BINARY(16) NOT NULL
);

-- Insert test data with a 16-byte binary value
INSERT INTO binary_data_type (id, binary_col)
VALUES (1, UNHEX('0123456789ABCDEF0123456789ABCDEF'));

CREATE TABLE animal (
                      ID     INT    NOT NULL AUTO_INCREMENT,
                      NAME   VARCHAR(100) NOT NULL UNIQUE,
                      IS_PET BOOLEAN      NOT NULL,
PRIMARY KEY (ID));

INSERT INTO animal (IS_PET, NAME) VALUES (TRUE, 'dog');
INSERT INTO animal (IS_PET, NAME) VALUES (TRUE, 'cat');
INSERT INTO animal (IS_PET, NAME) VALUES (FALSE, 'cow');
