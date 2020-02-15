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
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
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
public class JDBCConnectionImpl implements SQLConnection {

  private static final Logger log = LoggerFactory.getLogger(JDBCConnectionImpl.class);

  private final Connection conn;
  private final ContextInternal ctx;
  private final PoolMetrics metrics;
  private final Object metric;

  private final JDBCStatementHelper helper;

  private SQLOptions options;

  public JDBCConnectionImpl(Context context, JDBCStatementHelper helper, Connection conn, PoolMetrics metrics, Object metric) {
    this.helper = helper;
    this.conn = conn;
    this.metrics = metrics;
    this.metric = metric;
    this.ctx = (ContextInternal) context;
  }

  @Override
  public synchronized SQLConnection setOptions(SQLOptions options) {
    this.options = options;
    return this;
  }

  private synchronized SQLOptions getOptions() {
    return options;
  }

  @Override
  public SQLConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> resultHandler) {
    schedule(new JDBCAutoCommit(getOptions(), autoCommit)).onComplete(resultHandler);
    return this;
  }

  @Override
  public SQLConnection execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
    schedule(new JDBCExecute(getOptions(), sql)).onComplete(resultHandler);
    return this;
  }

  @Override
  public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    schedule(new JDBCQuery(helper, getOptions(), sql, null)).onComplete(resultHandler);
    return this;
  }

  @Override
  public SQLConnection queryStream(String sql, Handler<AsyncResult<SQLRowStream>> handler) {
    schedule(new StreamQuery(helper, getOptions(), ctx, sql, null)).onComplete(handler);
    return this;
  }

  @Override
  public SQLConnection queryStreamWithParams(String sql, JsonArray params, Handler<AsyncResult<SQLRowStream>> handler) {
    schedule(new StreamQuery(helper, getOptions(), ctx, sql, params)).onComplete(handler);
    return this;
  }

  @Override
  public SQLConnection queryWithParams(String sql, JsonArray params, Handler<AsyncResult<ResultSet>> resultHandler) {
    schedule(new JDBCQuery(helper, getOptions(), sql, params)).onComplete(resultHandler);
    return this;
  }

  @Override
  public SQLConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
    schedule(new JDBCUpdate(helper, getOptions(), sql, null)).onComplete(resultHandler);
    return this;
  }

  @Override
  public SQLConnection updateWithParams(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> resultHandler) {
    schedule(new JDBCUpdate(helper, getOptions(), sql, params)).onComplete(resultHandler);
    return this;
  }

  @Override
  public SQLConnection call(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    schedule(new JDBCCallable(helper, getOptions(), sql, null, null)).onComplete(resultHandler);
    return this;
  }

  @Override
  public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs, Handler<AsyncResult<ResultSet>> resultHandler) {
    schedule(new JDBCCallable(helper, getOptions(), sql, params, outputs)).onComplete(resultHandler);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    schedule(new JDBCClose(getOptions(), metrics, metric)).onComplete(handler);
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
    schedule(new JDBCCommit(getOptions())).onComplete(handler);
    return this;
  }

  @Override
  public SQLConnection rollback(Handler<AsyncResult<Void>> handler) {
    schedule(new JDBCRollback(getOptions())).onComplete(handler);
    return this;
  }

  @Override
  public SQLConnection getTransactionIsolation(Handler<AsyncResult<TransactionIsolation>> handler) {
    ctx.executeBlocking((Promise<TransactionIsolation> f) -> {
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
    }, handler);

    return this;
  }

  @Override
  public SQLConnection batch(List<String> sqlStatements, Handler<AsyncResult<List<Integer>>> handler) {
    schedule(new JDBCBatch(helper, getOptions(), sqlStatements)).onComplete(handler);
    return this;
  }

  @Override
  public SQLConnection batchWithParams(String statement, List<JsonArray> args, Handler<AsyncResult<List<Integer>>> handler) {
    schedule(new JDBCBatch(helper, getOptions(), statement, args)).onComplete(handler);
    return this;
  }

  @Override
  public SQLConnection batchCallableWithParams(String statement, List<JsonArray> inArgs, List<JsonArray> outArgs, Handler<AsyncResult<List<Integer>>> handler) {
    schedule(new JDBCBatch(helper, getOptions(), statement, inArgs, outArgs)).onComplete(handler);
    return this;
  }

  @Override
  public SQLConnection setTransactionIsolation(TransactionIsolation isolation, Handler<AsyncResult<Void>> handler) {
    ctx.executeBlocking((Promise<Void> f) -> {
      try {
        conn.setTransactionIsolation(isolation.getType());
        f.complete(null);
      } catch (SQLException e) {
        f.fail(e);
      }
    }, handler);

    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C> C unwrap() {
    return (C) conn;
  }

  public <T> Future<T> schedule(AbstractJDBCAction<T> action) {
    SQLOptions sqlOptions = getOptions();
    return ctx.executeBlocking(promise -> {
      try {
        // apply connection options
        applyConnectionOptions(conn, sqlOptions);
        // execute
        T result = action.execute(conn);
        promise.complete(result);
      } catch (SQLException e) {
        promise.fail(e);
      }
    });
  }

  private static void applyConnectionOptions(Connection conn, SQLOptions sqlOptions) throws SQLException {
    if (sqlOptions != null) {
      if (sqlOptions.isReadOnly()) {
        conn.setReadOnly(true);
      }
      if (sqlOptions.getCatalog() != null) {
        conn.setCatalog(sqlOptions.getCatalog());
      }
      if (sqlOptions.getSchema() != null) {
        conn.setSchema(sqlOptions.getSchema());
      }
    }
  }
}
