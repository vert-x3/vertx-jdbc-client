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
import io.vertx.core.internal.PromiseInternal;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.jdbcclient.SqlOptions;
import io.vertx.jdbcclient.impl.actions.*;
import io.vertx.sqlclient.internal.PreparedStatement;
import io.vertx.sqlclient.internal.QueryResultHandler;
import io.vertx.sqlclient.spi.DatabaseMetadata;
import io.vertx.sqlclient.spi.connection.Connection;
import io.vertx.sqlclient.spi.connection.ConnectionContext;
import io.vertx.sqlclient.spi.protocol.*;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;

public class ConnectionImpl implements Connection {

  final JDBCStatementHelper helper;
  final ContextInternal context;
  final java.sql.Connection conn;
  final ClientMetrics<?, ?, ?> metrics;
  final String user;
  final String database;
  final SocketAddress server;
  final SqlOptions sqlOptionsBackup;
  SqlOptions sqlOptions;
  private volatile ConnectionContext holder;
  private volatile boolean broken;
  private volatile boolean closing;
//  final TaskQueue statementsQueue = new TaskQueue();


  public ConnectionImpl(JDBCStatementHelper helper, ContextInternal context, SqlOptions sqlOptions, java.sql.Connection conn, ClientMetrics<?, ?, ?> metrics, String user, String database, SocketAddress server) {
    this.conn = conn;
    this.helper = helper;
    this.context = context;
    this.user = user;
    this.database = database;
    this.server = server;
    this.metrics = metrics;
    this.sqlOptionsBackup = sqlOptions;
    this.sqlOptions = null;
  }

  Future<Void> beforeUsage() {
    sqlOptions = new SqlOptions(sqlOptionsBackup);
    PromiseInternal<Void> promise = context.owner().promise();
    context.<Void>executeBlocking(() -> {
      try {
        conn.beginRequest();
      } catch (SQLException e) {
        throw reportException(e);
      }
      return null;
    }, false).onComplete(promise);
    return promise.future();
  }

  Future<Void> afterUsage() {
    sqlOptions = null;
    PromiseInternal<Void> promise = context.owner().promise();
    context.<Void>executeBlocking(() -> {
      try {
        conn.endRequest();
      } catch (SQLException e) {
        throw reportException(e);
      }
      return null;
    }, false).onComplete(promise);
    return promise.future();
  }

  /**
   * Latches connection-fatal failures and reports them to the connection holder.
   *
   * <p>A {@code java.sql.Connection} has no event channel: unlike the socket based clients a dead
   * JDBC connection cannot notify its {@link ConnectionContext} of the disconnection. Without this
   * hook a pooled connection that dies while pooled (database restart, server side idle timeout,
   * a pooling {@code DataSource} invalidating the physical connection) is recycled forever, and
   * every lease of it fails until the process is restarted. Reporting {@code handleClosed()} lets
   * the pool remove the connection, exactly as it does when a socket based connection is closed.
   */
  private SQLException reportException(SQLException e) {
    if (!closing && !broken && isFatal(e)) {
      broken = true;
      ConnectionContext h = holder;
      if (h != null) {
        context.runOnContext(v -> h.handleClosed());
      }
    }
    return e;
  }

  private boolean isFatal(SQLException e) {
    int depth = 0;
    for (Throwable t = e; t != null && depth++ < 16; t = t.getCause()) {
      if (t instanceof SQLNonTransientConnectionException || t instanceof SQLRecoverableException) {
        return true;
      }
      if (t instanceof SQLException) {
        String sqlState = ((SQLException) t).getSQLState();
        if (sqlState != null && sqlState.startsWith("08")) {
          return true;
        }
      }
    }
    try {
      return conn.isClosed();
    } catch (SQLException ignore) {
      return true;
    }
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
    if (broken) {
      return false;
    }
    try {
      // Connection#isClosed does not ping the server, it only reflects local state
      return !conn.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public boolean isSsl() {
    return false;
  }

  @Override
  public DatabaseMetadata databaseMetadata() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(ConnectionContext context) {
    this.holder = context;
  }

  @Override
  public void close(ConnectionContext holder, Completable<Void> promise) {
    closing = true;
    schedule(new JDBCClose(sqlOptions, null, null))
      .andThen(ar -> {
        if (metrics != null) {
          metrics.close();
        }
      })
      .onComplete(promise);
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
      try {
        // apply connection options
        applyConnectionOptions(conn, sqlOptions);
        // execute
        return action.execute(conn);
      } catch (SQLException e) {
        throw reportException(e);
      }
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
