/*
 * Copyright (c) 2011-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.ext.jdbc.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.impl.actions.*;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;

import java.sql.Connection;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
class JDBCConnectionImpl implements SQLConnection {

  private static final Logger log = LoggerFactory.getLogger(JDBCConnectionImpl.class);

  private final Vertx vertx;
  private final Connection conn;

  public JDBCConnectionImpl(Vertx vertx, Connection conn) {
    this.vertx = vertx;
    this.conn = conn;
  }

  @Override
  public SQLConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> resultHandler) {
    new JDBCAutoCommit(vertx, conn, autoCommit).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
    new JDBCExecute(vertx, conn, sql).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCQuery(vertx, conn, sql, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection queryWithParams(String sql, JsonArray params, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCQuery(vertx, conn, sql, params).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JDBCUpdate(vertx, conn, sql, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection updateWithParams(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JDBCUpdate(vertx, conn, sql, params).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection call(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCCallable(vertx, conn, sql, null, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCCallable(vertx, conn, sql, params, outputs).execute(resultHandler);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    new JDBCClose(vertx, conn).execute(handler);
  }

  @Override
  public void close() {
    close(ar -> {
      if (ar.failed()) {
        log.error("Failure in closing connection", ar.cause());
      }
    });
  }

  @Override
  public SQLConnection commit(Handler<AsyncResult<Void>> handler) {
    new JDBCCommit(vertx, conn).execute(handler);
    return this;
  }

  @Override
  public SQLConnection rollback(Handler<AsyncResult<Void>> handler) {
    new JDBCRollback(vertx, conn).execute(handler);
    return this;
  }
}
