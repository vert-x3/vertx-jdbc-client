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
package io.vertx.jdbcclient.impl;

import io.vertx.core.Completable;
import io.vertx.core.Future;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.jdbcclient.SqlOptions;
import io.vertx.jdbcclient.impl.actions.*;
import io.vertx.sqlclient.internal.Connection;
import io.vertx.sqlclient.internal.PreparedStatement;
import io.vertx.sqlclient.internal.QueryResultHandler;
import io.vertx.sqlclient.internal.command.*;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import java.sql.SQLException;

public class ConnectionImpl implements Connection {

  final JDBCStatementHelper helper;
  final ContextInternal context;
  final java.sql.Connection conn;
  final ClientMetrics<?, ?, ?> metrics;
  final String user;
  final String database;
  final SocketAddress server;
//  final TaskQueue statementsQueue = new TaskQueue();

  SqlOptions sqlOptions;

  public ConnectionImpl(JDBCStatementHelper helper, ContextInternal context, SqlOptions sqlOptions, java.sql.Connection conn, ClientMetrics<?, ?, ?> metrics, String user, String database, SocketAddress server) {
    this.conn = conn;
    this.helper = helper;
    this.context = context;
    this.sqlOptions = sqlOptions;
    this.user = user;
    this.database = database;
    this.server = server;
    this.metrics = metrics;
  }

  public java.sql.Connection getJDBCConnection() {
    return conn;
  }

  @Override
  public TracingPolicy tracingPolicy() {
    return TracingPolicy.PROPAGATE;
  }

  @Override
  public String database() {
    return database;
  }

  @Override
  public String user() {
    return user;
  }

  @Override
  public ClientMetrics metrics() {
    return metrics;
  }

  @Override
  public int pipeliningLimit() {
    return 1;
  }

  @Override
  public SocketAddress server() {
    return server;
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public boolean isSsl() {
    return false;
  }

  @Override
  public DatabaseMetadata getDatabaseMetaData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(Holder holder) {
  }

  @Override
  public void close(Holder holder, Completable<Void> promise) {
    schedule(new JDBCClose(sqlOptions, null, null))
      .andThen(ar -> {
        if (metrics != null) {
          metrics.close();
        }
      })
      .onComplete(promise);
  }

  @Override
  public int getProcessId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getSecretKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> void schedule(CommandBase<R> cmd, Completable<R> handler) {
    Future<R> fut;
    if (cmd instanceof SimpleQueryCommand<?>) {
      fut = (Future<R>) handle((SimpleQueryCommand<?>) cmd);
    } else if (cmd instanceof PrepareStatementCommand) {
      fut = (Future<R>) handle((PrepareStatementCommand) cmd);
    } else if (cmd instanceof ExtendedQueryCommand) {
      fut = (Future<R>) handle((ExtendedQueryCommand<?>) cmd);
    } else if (cmd instanceof TxCommand) {
      fut = handle((TxCommand<R>) cmd);
    } else if (cmd instanceof JDBCAction) {
      fut = schedule((JDBCAction<R>) cmd);
    } else {
      fut = Future.failedFuture("Not yet implemented " + cmd);
    }
    fut.onComplete(handler);
  }

  private Future<PreparedStatement> handle(PrepareStatementCommand command) {
    JDBCPrepareStatementAction action = new JDBCPrepareStatementAction(helper, sqlOptions, command.options(), command.sql());
    return schedule(action);
  }

  private <R> Future<Boolean> handle(ExtendedQueryCommand<R> command) {
    JDBCQueryAction<?, R> action =
      command.isBatch() ?
        new JDBCPreparedBatch<>(helper, sqlOptions, command.options(), command, command.collector(), command.paramsList()) :
        new JDBCPreparedQuery<>(helper, sqlOptions, command.options(), command, command.collector(), command.params());

    return handle(action, command.resultHandler());
  }

  private <R> Future<Boolean> handle(SimpleQueryCommand<R> command) {
    JDBCQueryAction<?, R> action = new JDBCSimpleQueryAction<>(helper, sqlOptions, command.sql(), command.collector());
    return handle(action, command.resultHandler());
  }

  private <R> Future<R> handle(TxCommand<R> command) {
    JDBCTxOp<R> action = new JDBCTxOp<>(helper, command, sqlOptions);
    return schedule(action);
  }

  private <R> Future<Boolean> handle(JDBCQueryAction<?, R> action, QueryResultHandler<R> handler) {
    return schedule(action)
      .map(response -> {
        response.handle(handler);
        return false;
      });
  }

  public <T> Future<T> schedule(JDBCAction<T> action) {
    return context.executeBlocking(() -> {
      // apply connection options
      applyConnectionOptions(conn, sqlOptions);
      // execute
      return action.execute(conn);
    }/*, statementsQueue*/);
  }

  public static void applyConnectionOptions(java.sql.Connection conn, SqlOptions sqlOptions) throws SQLException {
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
