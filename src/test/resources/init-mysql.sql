/*
 * Copyright (c) 2011-2026 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

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

CREATE TABLE people
(
  ID   INT          NOT NULL AUTO_INCREMENT,
  NAME VARCHAR(100) NOT NULL,
  PRIMARY KEY (ID)
);
