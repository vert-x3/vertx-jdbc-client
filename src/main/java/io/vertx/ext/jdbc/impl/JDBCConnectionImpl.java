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
import io.vertx.core.WorkerExecutor;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.impl.actions.*;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.TransactionIsolation;
import io.vertx.ext.sql.UpdateResult;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
class JDBCConnectionImpl implements SQLConnection {

  private static final Logger log = LoggerFactory.getLogger(JDBCConnectionImpl.class);

  private final Vertx vertx;
  private final Connection conn;
  private final WorkerExecutor executor;

  private int timeout = -1;

  public JDBCConnectionImpl(Vertx vertx, Connection conn) {
    this.vertx = vertx;
    this.conn = conn;
    this.executor = ((ContextInternal) vertx.getOrCreateContext()).createWorkerExecutor();
  }

  @Override
  public SQLConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> resultHandler) {
    new JDBCAutoCommit(vertx, conn, executor, autoCommit).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
    new JDBCExecute(vertx, conn, executor, timeout, sql).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCQuery(vertx, conn, executor, timeout, sql, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection queryWithParams(String sql, JsonArray params, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCQuery(vertx, conn, executor, timeout, sql, params).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JDBCUpdate(vertx, conn, executor, timeout, sql, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection updateWithParams(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> resultHandler) {
    new JDBCUpdate(vertx, conn, executor, timeout, sql, params).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection call(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCCallable(vertx, conn, executor, timeout, sql, null, null).execute(resultHandler);
    return this;
  }

  @Override
  public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs, Handler<AsyncResult<ResultSet>> resultHandler) {
    new JDBCCallable(vertx, conn, executor, timeout, sql, params, outputs).execute(resultHandler);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    new JDBCClose(vertx, conn, executor).execute(handler);
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
    new JDBCCommit(vertx, conn, executor).execute(handler);
    return this;
  }

  @Override
  public SQLConnection rollback(Handler<AsyncResult<Void>> handler) {
    new JDBCRollback(vertx, conn, executor).execute(handler);
    return this;
  }

  @Override
  public SQLConnection setQueryTimeout(int timeoutInSeconds) {
    this.timeout = timeoutInSeconds;
    return this;
  }

  @Override
  public SQLConnection setTransactionIsolation(TransactionIsolation isolation, Handler<AsyncResult<Void>> handler) {
    Future<Void> f = Future.future();
    final Context callbackContext = vertx.getOrCreateContext();
    context.runOnContext(v -> {
      f.setHandler(ar -> callbackContext.runOnContext(v2 -> handler.handle(ar)));
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
    });

    return this;
  }

  @Override
  public SQLConnection getTransactionIsolation(Handler<AsyncResult<TransactionIsolation>> handler) {
    Future<TransactionIsolation> f = Future.future();
    final Context callbackContext = vertx.getOrCreateContext();
    context.runOnContext(v -> {
      f.setHandler(ar -> callbackContext.runOnContext(v2 -> handler.handle(ar)));
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
    });

    return this;
  }
}
