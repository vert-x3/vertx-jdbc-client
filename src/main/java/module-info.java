module io.vertx.client.jdbc {

  requires static io.vertx.codegen.api;
  requires static io.vertx.codegen.json;
  requires static io.vertx.docgen;

  requires io.vertx.core;
  requires io.vertx.core.logging;
  requires io.vertx.client.sql;
  requires java.sql;

  exports io.vertx.jdbcclient;
  exports io.vertx.jdbcclient.spi;

  uses JDBCEncoder;
  uses JDBCDecoder;

}
