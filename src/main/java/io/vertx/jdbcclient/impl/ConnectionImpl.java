package io.vertx.jdbcclient.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.jdbcclient.impl.actions.JDBCPrepareStatementAction;
import io.vertx.jdbcclient.impl.actions.JDBCPreparedQuery;
import io.vertx.jdbcclient.impl.actions.JDBCPreparedStatement;
import io.vertx.jdbcclient.impl.actions.JDBCQueryAction;
import io.vertx.jdbcclient.impl.actions.JDBCSimpleQueryAction;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.PreparedStatement;
import io.vertx.sqlclient.impl.QueryResultHandler;
import io.vertx.sqlclient.impl.command.BiCommand;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.ext.jdbc.impl.JDBCConnectionImpl;
import io.vertx.sqlclient.impl.command.ExtendedQueryCommand;
import io.vertx.sqlclient.impl.command.PrepareStatementCommand;
import io.vertx.sqlclient.impl.command.SimpleQueryCommand;

public class ConnectionImpl implements Connection {

  final JDBCStatementHelper helper;
  final ContextInternal context;
  final JDBCConnectionImpl conn;
  private Holder holder;

  public ConnectionImpl(JDBCStatementHelper helper, ContextInternal context, JDBCConnectionImpl conn) {
    this.conn = conn;
    this.helper = helper;
    this.context = context;
  }

  @Override
  public boolean isSsl() {
    return false;
  }

  @Override
  public void init(Holder holder) {
    this.holder = holder;
  }

  @Override
  public void close(Holder holder) {

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
    } else if (commandBase instanceof BiCommand<?, ?>) {
      handle((BiCommand) commandBase, promise);
    } else {
      promise.fail("Not yet implemented " + commandBase);
    }
  }

  private void handle(PrepareStatementCommand command, Promise<PreparedStatement> promise) {
    JDBCPrepareStatementAction action = new JDBCPrepareStatementAction(helper, null, command.sql());
    Future<PreparedStatement> fut = conn.schedule(action);
    fut.setHandler(promise);
  }

  private <R> void handle(ExtendedQueryCommand<R> command, Promise<Boolean> promise) {
    JDBCPreparedStatement jdbcPreparedStatement = (JDBCPreparedStatement) command.preparedStatement();
    JDBCPreparedQuery<?, R> action = new JDBCPreparedQuery<>(helper, null, jdbcPreparedStatement.preparedStatement(), jdbcPreparedStatement.sql(), command.collector(), command.params());
    handle(action, command.resultHandler(), promise);
  }

  private <R> void handle(SimpleQueryCommand<R> command, Promise<Boolean> promise) {
    JDBCSimpleQueryAction<?, R> action = new JDBCSimpleQueryAction<>(helper, null, command.sql(), command.collector());
    handle(action, command.resultHandler(), promise);
  }

  private <R> void handle(JDBCQueryAction<?, R> action, QueryResultHandler<R> handler, Promise<Boolean> promise) {
    Future<JDBCSimpleQueryAction.Response<R>> fut = conn.schedule(action);
    fut.setHandler(ar -> {
      if (ar.succeeded()) {
        JDBCSimpleQueryAction.Response<R> resp = ar.result();
        handler.handleResult(0, resp.size, resp.rowDesc, resp.result, null);
        promise.complete(true);
      } else {
        System.out.println("HANDLE ME " + ar.succeeded());
        ar.cause().printStackTrace();
      }
    });
  }

  private <T, R> void handle(BiCommand<T, R> command, Promise<R> promise) {
    Promise<T> p = Promise.promise();
    p.future().setHandler(ar -> {
      if (ar.succeeded()) {
        AsyncResult<CommandBase<R>> b = command.then.apply(ar.result());
        if (b.succeeded()) {
          schedule(b.result(), promise);
        } else {
          promise.fail(b.cause());
        }
      } else {
        promise.fail(ar.cause());
      }
    });
    schedule(command.first, p);
  }
}
