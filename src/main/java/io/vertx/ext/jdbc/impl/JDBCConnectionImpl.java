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
 */
class JDBCConnectionImpl implements SQLConnection {

  private static final Logger log = LoggerFactory.getLogger(JDBCConnectionImpl.class);

  private final Vertx vertx;
  private final Connection conn;
  private final ContextInternal ctx;
  private final PoolMetrics metrics;
  private final Object metric;
  private final int rowStreamFetchSize;
  private final TaskQueue statementsQueue = new TaskQueue();

  private final JDBCStatementHelper helper;

  private int timeout = -1;

  public JDBCConnectionImpl(Context context, JDBCStatementHelper helper, Connection conn, PoolMetrics metrics, Object metric, int rowStreamFetchSize) {
    this.vertx = context.owner();
    this.helper = helper;
    this.conn = conn;
    this.metrics = metrics;
    this.metric = metric;
    this.rowStreamFetchSize = rowStreamFetchSize;
    this.ctx = ((ContextInternal)context);
  }

  @Override
  public SQLConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> resultHandler) {
    new JDBCAutoCommit(vertx, conn, ctx, statementsQueue, autoCommit).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
    new JDBCExecute(vertx, conn, ctx, statementsQueue, timeout, sql).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCQuery(vertx, helper, conn, ctx, statementsQueue, timeout, sql, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection queryStream(String sql, Handler<AsyncResult<SQLRowStream>> handler) {
    new StreamQuery(vertx, helper, conn, ctx, statementsQueue, timeout, rowStreamFetchSize, sql, null).execute(handler);
    return this;
  }

  @Override
  public SQLConnection queryStreamWithParams(String sql, JsonArray params, Handler<AsyncResult<SQLRowStream>> handler) {
    new StreamQuery(vertx, helper, conn, ctx, statementsQueue, timeout, rowStreamFetchSize, sql, params).execute(handler);
    return this;
  }

  @Override
  public SQLConnection queryWithParams(String sql, JsonArray params, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCQuery(vertx, helper, conn, ctx, statementsQueue, timeout, sql, params).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JDBCUpdate(vertx, helper, conn, ctx, statementsQueue, timeout, sql, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection updateWithParams(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JDBCUpdate(vertx, helper, conn, ctx, statementsQueue, timeout, sql, params).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection call(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCCallable(vertx, helper, conn, ctx, statementsQueue, timeout, sql, null, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCCallable(vertx, helper, conn, ctx, statementsQueue, timeout, sql, params, outputs).execute(resultHandler);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    if (metrics != null) {
      metrics.end(metric, true);
    }
    new JDBCClose(vertx, conn, ctx, statementsQueue).execute(handler);
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
    new JDBCCommit(vertx, conn, ctx, statementsQueue).execute(handler);
    return this;
  }

  @Override
  public SQLConnection rollback(Handler<AsyncResult<Void>> handler) {
    new JDBCRollback(vertx, conn, ctx, statementsQueue).execute(handler);
    return this;
  }

  @Override
  public SQLConnection setQueryTimeout(int timeoutInSeconds) {
    this.timeout = timeoutInSeconds;
    return this;
  }

  @Override
  public SQLConnection setTransactionIsolation(TransactionIsolation isolation, Handler<AsyncResult<Void>> handler) {
    ctx.executeBlocking((Future<Void> f) -> {
      try {
        switch (isolation) {
          case READ_COMMITTED:
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            break;
          case READ_UNCOMMITTED:
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            break;
          case REPEATABLE_READ:
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            break;
          case SERIALIZABLE:
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            break;
          case NONE:
            conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
            break;
          default:
            log.warn("Unknown isolation level " + isolation.name());
        }
        f.complete();
      } catch (SQLException e) {
        f.fail(e);
      }
    }, statementsQueue, handler);

    return this;
  }

  @Override
  public SQLConnection getTransactionIsolation(Handler<AsyncResult<TransactionIsolation>> handler) {
    ctx.executeBlocking((Future<TransactionIsolation> f) -> {
      try {
        int level = conn.getTransactionIsolation();

        switch (level) {
          case Connection.TRANSACTION_READ_COMMITTED:
            f.complete(TransactionIsolation.READ_COMMITTED);
            break;
          case Connection.TRANSACTION_READ_UNCOMMITTED:
            f.complete(TransactionIsolation.READ_UNCOMMITTED);
            break;
          case Connection.TRANSACTION_REPEATABLE_READ:
            f.complete(TransactionIsolation.REPEATABLE_READ);
            break;
          case Connection.TRANSACTION_SERIALIZABLE:
            f.complete(TransactionIsolation.SERIALIZABLE);
            break;
          case Connection.TRANSACTION_NONE:
            f.complete(TransactionIsolation.NONE);
            break;
          default:
            f.fail("Unknown isolation level " + level);
            break;
        }
      } catch (SQLException e) {
        f.fail(e);
      }
    }, statementsQueue, handler);

    return this;
  }

  @Override
  public SQLConnection batch(List<String> sqlStatements, Handler<AsyncResult<List<Integer>>> handler) {
    new JDBCBatch(vertx, helper, conn, ctx, statementsQueue, sqlStatements).execute(handler);
    return this;
  }

  @Override
  public SQLConnection batchWithParams(String statement, List<JsonArray> args, Handler<AsyncResult<List<Integer>>> handler) {
    new JDBCBatch(vertx, helper, conn, ctx, statementsQueue, statement, args).execute(handler);
    return this;
  }

  @Override
  public SQLConnection batchCallableWithParams(String statement, List<JsonArray> inArgs, List<JsonArray> outArgs, Handler<AsyncResult<List<Integer>>> handler) {
    new JDBCBatch(vertx, helper, conn, ctx, statementsQueue, statement, inArgs, outArgs).execute(handler);
    return this;
  }
}
