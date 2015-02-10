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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.actions.JdbcAutoCommit;
import io.vertx.ext.jdbc.impl.actions.JdbcClose;
import io.vertx.ext.jdbc.impl.actions.JdbcCommit;
import io.vertx.ext.jdbc.impl.actions.JdbcExecute;
import io.vertx.ext.jdbc.impl.actions.JdbcQuery;
import io.vertx.ext.jdbc.impl.actions.JdbcRollback;
import io.vertx.ext.jdbc.impl.actions.JdbcUpdate;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SqlConnection;
import io.vertx.ext.sql.UpdateResult;

import java.sql.Connection;
import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
class JdbcConnectionImpl implements SqlConnection {

  private final Vertx vertx;
  private final Connection conn;

  public JdbcConnectionImpl(Vertx vertx, Connection conn) {
    this.vertx = vertx;
    this.conn = conn;
  }

  @Override
  public SqlConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> resultHandler) {
    new JdbcAutoCommit(vertx, conn, autoCommit).execute(resultHandler);
    return this;
  }

  @Override
  public SqlConnection execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
    new JdbcExecute(vertx, conn, sql).execute(resultHandler);
    return this;
  }

  @Override
  public SqlConnection query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JdbcQuery(vertx, conn, sql, null).execute(resultHandler);
    return this;
  }

  @Override
  public SqlConnection queryWithParams(String sql, JsonArray params, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JdbcQuery(vertx, conn, sql, params).execute(resultHandler);
    return this;
  }

  @Override
  public SqlConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JdbcUpdate(vertx, conn, sql, null).execute(resultHandler);
    return this;
  }

  @Override
  public SqlConnection updateWithParams(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JdbcUpdate(vertx, conn, sql, params).execute(resultHandler);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    new JdbcClose(vertx, conn).execute(handler);
  }

  @Override
  public SqlConnection commit(Handler<AsyncResult<Void>> handler) {
    new JdbcCommit(vertx, conn).execute(handler);
    return this;
  }

  @Override
  public SqlConnection rollback(Handler<AsyncResult<Void>> handler) {
    new JdbcRollback(vertx, conn).execute(handler);
    return this;
  }
}
