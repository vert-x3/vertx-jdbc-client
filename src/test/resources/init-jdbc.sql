CREATE TABLE product
(
    id       VARCHAR2(20)  NOT NULL,
    name     VARCHAR2(100) NOT NULL,
    url      VARCHAR2(100),
    company  VARCHAR2(100),
    area     VARCHAR2(100),
    category VARCHAR2(100),
    channel  VARCHAR2(10),
    active   NUMBER(1, 0)  NOT NULL,
    CONSTRAINT product_pk PRIMARY KEY (id)
);

INSERT INTO product(id, name, url, company, area, category, channel, active) VALUES ('anId', 'name', 'url', 'company', 'area', 'category', 'channel', 1)
