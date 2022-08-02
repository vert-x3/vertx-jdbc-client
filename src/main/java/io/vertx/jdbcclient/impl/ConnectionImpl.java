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
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.jdbc.impl.JDBCConnectionImpl;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.jdbcclient.impl.actions.*;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.PreparedStatement;
import io.vertx.sqlclient.impl.QueryResultHandler;
import io.vertx.sqlclient.impl.command.*;
import io.vertx.sqlclient.spi.DatabaseMetadata;

public class ConnectionImpl implements Connection {

  final JDBCStatementHelper helper;
  final ContextInternal context;
  final JDBCConnectionImpl conn;
  final SQLOptions sqlOptions;

  public ConnectionImpl(JDBCStatementHelper helper, ContextInternal context, SQLOptions sqlOptions, JDBCConnectionImpl conn) {
    this.conn = conn;
    this.helper = helper;
    this.context = context;
    this.sqlOptions = sqlOptions;
  }

  @Override
  public SocketAddress server() {
    throw new UnsupportedOperationException("Not yet implemented");
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
    conn.close(promise);
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
    return conn.schedule(action);
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
    return conn.schedule(action);
  }

  private <R> Future<Boolean> handle(JDBCQueryAction<?, R> action, QueryResultHandler<R> handler) {
    return conn.schedule(action)
      .map(response -> {
        response.handle(handler);
        return false;
      });
  }
}
