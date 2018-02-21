package io.vertx.ext.jdbc;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.*;

import java.util.List;

/**
 * @author <a href="mailto:ruslan.sennov@gmail.com">Ruslan Sennov</a>
 */
public class CloseConnectionChecker implements JDBCClient {

  private final SQLClient delegate;
  private final Handler<Void> onConnectionClosed;

  CloseConnectionChecker(SQLClient delegate, Handler<Void> onConnectionClosed) {
    this.delegate = delegate;
    this.onConnectionClosed = onConnectionClosed;
  }

  @Override
  public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
    delegate.getConnection(conn -> {
      if (conn.succeeded()) {
        handler.handle(Future.succeededFuture(new SQLConnectionWrapper(conn.result())));
      } else {
        handler.handle(conn);
      }
    });
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    delegate.close(handler);
  }

  @Override
  public void close() {
    delegate.close();
  }

  private class SQLConnectionWrapper implements SQLConnection {

    private final SQLConnection delegate;

    SQLConnectionWrapper(SQLConnection delegate) {
      this.delegate = delegate;
    }

    @Override
    public SQLConnection setOptions(SQLOptions options) {
      delegate.setOptions(options);
      return this;
    }

    @Override
    public SQLConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> resultHandler) {
      delegate.setAutoCommit(autoCommit, resultHandler);
      return this;
    }

    @Override
    public SQLConnection execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
      delegate.execute(sql, resultHandler);
      return this;
    }

    @Override
    public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
      delegate.query(sql, resultHandler);
      return this;
    }

    @Override
    public SQLConnection queryStream(String sql, Handler<AsyncResult<SQLRowStream>> handler) {
      delegate.queryStream(sql, handler);
      return this;
    }

    @Override
    public SQLConnection queryWithParams(String sql, JsonArray params, Handler<AsyncResult<ResultSet>> resultHandler) {
      delegate.queryWithParams(sql, params, resultHandler);
      return this;
    }

    @Override
    public SQLConnection queryStreamWithParams(String sql, JsonArray params, Handler<AsyncResult<SQLRowStream>> handler) {
      delegate.queryStreamWithParams(sql, params, handler);
      return this;
    }

    @Override
    public SQLConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
      delegate.update(sql, resultHandler);
      return this;
    }

    @Override
    public SQLConnection updateWithParams(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> resultHandler) {
      delegate.updateWithParams(sql, params, resultHandler);
      return this;
    }

    @Override
    public SQLConnection call(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
      delegate.call(sql, resultHandler);
      return this;
    }

    @Override
    public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs, Handler<AsyncResult<ResultSet>> resultHandler) {
      delegate.callWithParams(sql, params, outputs, resultHandler);
      return this;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
      delegate.close(ar -> {
        if (ar.succeeded()) {
          handler.handle(ar);
          onConnectionClosed.handle(null);
        } else {
          handler.handle(ar);
        }
      });
    }

    @Override
    public void close() {
      delegate.close();
      onConnectionClosed.handle(null);
    }

    @Override
    public SQLConnection commit(Handler<AsyncResult<Void>> handler) {
      delegate.commit(handler);
      return this;
    }

    @Override
    public SQLConnection rollback(Handler<AsyncResult<Void>> handler) {
      delegate.rollback(handler);
      return this;
    }

    @Override
    public SQLConnection batch(List<String> sqlStatements, Handler<AsyncResult<List<Integer>>> handler) {
      delegate.batch(sqlStatements, handler);
      return this;
    }

    @Override
    public SQLConnection batchWithParams(String sqlStatement, List<JsonArray> args, Handler<AsyncResult<List<Integer>>> handler) {
      delegate.batchWithParams(sqlStatement, args, handler);
      return this;
    }

    @Override
    public SQLConnection batchCallableWithParams(String sqlStatement, List<JsonArray> inArgs, List<JsonArray> outArgs, Handler<AsyncResult<List<Integer>>> handler) {
      delegate.batchCallableWithParams(sqlStatement, inArgs, outArgs, handler);
      return this;
    }

    @Override
    public SQLConnection setTransactionIsolation(TransactionIsolation isolation, Handler<AsyncResult<Void>> handler) {
      delegate.setTransactionIsolation(isolation, handler);
      return this;
    }

    @Override
    public SQLConnection getTransactionIsolation(Handler<AsyncResult<TransactionIsolation>> handler) {
      delegate.getTransactionIsolation(handler);
      return this;
    }
  }
}
