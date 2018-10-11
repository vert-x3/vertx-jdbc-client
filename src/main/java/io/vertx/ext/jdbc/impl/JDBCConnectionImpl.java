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

import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.ext.jdbc.impl.actions.*;
import io.vertx.ext.sql.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
class JDBCConnectionImpl implements SQLConnection {

  static final Logger log = LoggerFactory.getLogger(JDBCConnectionImpl.class);

  private final Vertx vertx;
  final Connection conn;
  private final ContextInternal ctx;
  private final PoolMetrics metrics;
  final Object metric;
  private final TaskQueue statementsQueue = new TaskQueue();

  private final JDBCStatementHelper helper;

  private SQLOptions options;

  public JDBCConnectionImpl(Context context, JDBCStatementHelper helper, Connection conn, PoolMetrics metrics, Object metric) {
    this.vertx = context.owner();
    this.helper = helper;
    this.conn = conn;
    this.metrics = metrics;
    this.metric = metric;
    this.ctx = (ContextInternal) context;
  }

  @Override
  public SQLConnection setOptions(SQLOptions options) {
    this.options = options;
    return this;
  }

  @Override
  public SQLConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> resultHandler) {
    new JDBCAutoCommit(vertx, options, ctx, autoCommit).execute(conn, statementsQueue, resultHandler);
    return this;
  }

  @Override
  public SQLConnection execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
    new JDBCExecute(vertx, options, ctx, sql).execute(conn, statementsQueue, resultHandler);
    return this;
  }

  @Override
  public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCQuery(vertx, helper, options, ctx, sql, null).execute(conn, statementsQueue, resultHandler);
    return this;
  }

  @Override
  public SQLConnection queryStream(String sql, Handler<AsyncResult<SQLRowStream>> handler) {
    new StreamQuery(vertx, helper, options, ctx, statementsQueue, sql, null).execute(conn, statementsQueue,  handler);
    return this;
  }

  @Override
  public SQLConnection queryStreamWithParams(String sql, JsonArray params, Handler<AsyncResult<SQLRowStream>> handler) {
    new StreamQuery(vertx, helper, options, ctx, statementsQueue, sql, params).execute(conn, statementsQueue,  handler);
    return this;
  }

  @Override
  public SQLConnection queryWithParams(String sql, JsonArray params, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCQuery(vertx, helper, options, ctx, sql, params).execute(conn, statementsQueue, resultHandler);
    return this;
  }

  @Override
  public SQLConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JDBCUpdate(vertx, helper, options, ctx, sql, null).execute(conn, statementsQueue, resultHandler);
    return this;
  }

  @Override
  public SQLConnection updateWithParams(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JDBCUpdate(vertx, helper, options, ctx, sql, params).execute(conn, statementsQueue, resultHandler);
    return this;
  }

  @Override
  public SQLConnection call(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCCallable(vertx, helper, options, ctx, sql, null, null).execute(conn, statementsQueue, resultHandler);
    return this;
  }

  @Override
  public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCCallable(vertx, helper, options, ctx, sql, params, outputs).execute(conn, statementsQueue, resultHandler);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    new JDBCClose(vertx, options, ctx, metrics, metric).execute(conn, statementsQueue, handler);
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
    new JDBCCommit(vertx, options, ctx).execute(conn, statementsQueue,  handler);
    return this;
  }

  @Override
  public SQLConnection rollback(Handler<AsyncResult<Void>> handler) {
    new JDBCRollback(vertx, options, ctx).execute(conn, statementsQueue,  handler);
    return this;
  }

  @Override
  public SQLConnection getTransactionIsolation(Handler<AsyncResult<TransactionIsolation>> handler) {
    ctx.executeBlocking((Future<TransactionIsolation> f) -> {
      try {
        TransactionIsolation txIsolation = TransactionIsolation.from(conn.getTransactionIsolation());

        if (txIsolation != null) {
          f.complete(txIsolation);
        } else {
          f.fail("Unknown isolation level");
        }
      } catch (SQLException e) {
        f.fail(e);
      }
    }, statementsQueue, handler);

    return this;
  }

  @Override
  public SQLConnection batch(List<String> sqlStatements, Handler<AsyncResult<List<Integer>>> handler) {
    new JDBCBatch(vertx, helper, options, ctx, sqlStatements).execute(conn, statementsQueue,  handler);
    return this;
  }

  @Override
  public SQLConnection batchWithParams(String statement, List<JsonArray> args, Handler<AsyncResult<List<Integer>>> handler) {
    new JDBCBatch(vertx, helper, options, ctx, statement, args).execute(conn, statementsQueue,  handler);
    return this;
  }

  @Override
  public SQLConnection batchCallableWithParams(String statement, List<JsonArray> inArgs, List<JsonArray> outArgs, Handler<AsyncResult<List<Integer>>> handler) {
    new JDBCBatch(vertx, helper, options, ctx, statement, inArgs, outArgs).execute(conn, statementsQueue,  handler);
    return this;
  }

  @Override
  public SQLConnection setTransactionIsolation(TransactionIsolation isolation, Handler<AsyncResult<Void>> handler) {
    ctx.executeBlocking((Future<Void> f) -> {
      try {
        conn.setTransactionIsolation(isolation.getType());
        f.complete(null);
      } catch (SQLException e) {
        f.fail(e);
      }
    }, statementsQueue, handler);

    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C> C unwrap() {
    return (C) conn;
  }
}
