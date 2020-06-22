package io.vertx.jdbcclient.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.impl.actions.JDBCTxOp;
import io.vertx.jdbcclient.impl.actions.JDBCPrepareStatementAction;
import io.vertx.jdbcclient.impl.actions.JDBCPreparedQuery;
import io.vertx.jdbcclient.impl.actions.JDBCPreparedStatement;
import io.vertx.jdbcclient.impl.actions.JDBCQueryAction;
import io.vertx.jdbcclient.impl.actions.JDBCSimpleQueryAction;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.PreparedStatement;
import io.vertx.sqlclient.impl.QueryResultHandler;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.ext.jdbc.impl.JDBCConnectionImpl;
import io.vertx.sqlclient.impl.command.ExtendedQueryCommand;
import io.vertx.sqlclient.impl.command.PrepareStatementCommand;
import io.vertx.sqlclient.impl.command.SimpleQueryCommand;
import io.vertx.sqlclient.impl.command.TxCommand;
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
    conn.close(ar -> {
      if (ar.succeeded()) {
        promise.complete();
      } else {
        promise.fail(ar.cause());
      }
    });
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
  public <R> void schedule(CommandBase<R> commandBase, Promise<R> promise) {
    if (commandBase instanceof SimpleQueryCommand<?>) {
      handle((SimpleQueryCommand<?>) commandBase, (Promise<Boolean>) promise);
    } else if (commandBase instanceof PrepareStatementCommand) {
      handle((PrepareStatementCommand) commandBase, (Promise<PreparedStatement>) promise);
    } else if (commandBase instanceof ExtendedQueryCommand) {
      handle((ExtendedQueryCommand<?>) commandBase, (Promise<Boolean>) promise);
    } else if (commandBase instanceof TxCommand) {
      handle((TxCommand<R>) commandBase, promise);
    } else {
      promise.fail("Not yet implemented " + commandBase);
    }
  }

  private void handle(PrepareStatementCommand command, Promise<PreparedStatement> promise) {
    JDBCPrepareStatementAction action = new JDBCPrepareStatementAction(helper, sqlOptions, command.sql());
    Future<PreparedStatement> fut = conn.schedule(action);
    fut.onComplete(promise);
  }

  private <R> void handle(ExtendedQueryCommand<R> command, Promise<Boolean> promise) {
    JDBCPreparedQuery<?, R> action = new JDBCPreparedQuery<>(helper, sqlOptions, command, command.collector(), command.params());
    handle(action, command.resultHandler(), promise);
  }

  private <R> void handle(SimpleQueryCommand<R> command, Promise<Boolean> promise) {
    JDBCSimpleQueryAction<?, R> action = new JDBCSimpleQueryAction<>(helper, sqlOptions, command.sql(), command.collector());
    handle(action, command.resultHandler(), promise);
  }

  private <R> void handle(TxCommand<R> command, Promise<R> promise) {
    JDBCTxOp<R> action = new JDBCTxOp<>(helper, command, sqlOptions);
    Future<R> fut = conn.schedule(action);
    fut.onComplete(promise);
  }

  private <R> void handle(JDBCQueryAction<?, R> action, QueryResultHandler<R> handler, Promise<Boolean> promise) {
    Future<JDBCSimpleQueryAction.Response<R>> fut = conn.schedule(action);
    fut.onComplete(ar -> {
      if (ar.succeeded()) {
        JDBCSimpleQueryAction.Response<R> resp = ar.result();
        handler.handleResult(resp.updatedRows, resp.size, resp.rowDesc, resp.result, null);
        if (resp.generatedIds != null) {
          handler.addProperty(JDBCPool.GENERATED_KEYS, resp.generatedIds);
        }
        promise.complete(true);
      } else {
        promise.fail(ar.cause());
      }
    });
  }
}
