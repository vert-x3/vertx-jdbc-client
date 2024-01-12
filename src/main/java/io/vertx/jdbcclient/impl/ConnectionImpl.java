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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.jdbc.impl.actions.AbstractJDBCAction;
import io.vertx.ext.jdbc.impl.actions.JDBCAction;
import io.vertx.ext.jdbc.impl.actions.JDBCClose;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.jdbcclient.impl.actions.*;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.PreparedStatement;
import io.vertx.sqlclient.impl.QueryResultHandler;
import io.vertx.sqlclient.impl.command.*;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import java.util.function.Consumer;

import static io.vertx.ext.jdbc.impl.JDBCConnectionImpl.applyConnectionOptions;

public class ConnectionImpl implements Connection {

  final JDBCStatementHelper helper;
  final ContextInternal context;
  final java.sql.Connection conn;
  final ClientMetrics<?, ?, ?, ?> metrics;
  final SQLOptions sqlOptions;
  final String user;
  final String database;
  final SocketAddress server;
  final TaskQueue statementsQueue = new TaskQueue();

  public ConnectionImpl(JDBCStatementHelper helper, ContextInternal context, SQLOptions sqlOptions, java.sql.Connection conn, ClientMetrics<?, ?, ?, ?> metrics, String user, String database, SocketAddress server) {
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
  public void close(Holder holder, Promise<Void> promise) {
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
  public <R> Future<R> schedule(ContextInternal contextInternal, CommandBase<R> commandBase) {
    if (commandBase instanceof SimpleQueryCommand<?>) {
      return (Future<R>)handle((SimpleQueryCommand<?>) commandBase);
    } else if (commandBase instanceof PrepareStatementCommand) {
      return (Future<R>) handle((PrepareStatementCommand) commandBase);
    } else if (commandBase instanceof ExtendedQueryCommand) {
      return (Future<R>) handle((ExtendedQueryCommand<?>) commandBase);
    } else if (commandBase instanceof TxCommand) {
      return handle((TxCommand<R>) commandBase);
    } else {
      return Future.failedFuture("Not yet implemented " + commandBase);
    }
  }

  private Future<PreparedStatement> handle(PrepareStatementCommand command) {
    JDBCPrepareStatementAction action = new JDBCPrepareStatementAction(helper, sqlOptions, command.sql());
    return schedule(action);
  }

  private <R> Future<Boolean> handle(ExtendedQueryCommand<R> command) {
    JDBCQueryAction<?, R> action =
      command.isBatch() ?
        new JDBCPreparedBatch<>(helper, sqlOptions, command, command.collector(), command.paramsList()) :
        new JDBCPreparedQuery<>(helper, sqlOptions, command, command.collector(), command.params());

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
    }, statementsQueue);
  }
}
