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

package io.vertx.ext.sql;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.impl.RowStreamWrapper;

/**
 * A common asynchronous client interface for interacting with SQL compliant database
 *
 * @author <a href="mailto:emad.albloushi@gmail.com">Emad Alblueshi</a>
 */
@VertxGen
public interface SQLClient extends SQLOperations {

  /**
   * Returns a connection that can be used to perform SQL operations on. It's important to remember
   * to close the connection when you are done, so it is returned to the pool.
   *
   * @param handler the handler which is called when the <code>JdbcConnection</code> object is ready for use.
   */
  @Fluent
  SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler);

  /**
   * Close the client and release all resources.
   * Call the handler when close is complete.
   *
   * @param handler the handler that will be called when close is complete
   */
  void close(Handler<AsyncResult<Void>> handler);

  /**
   * Close the client
   */
  void close();

  /**
   * Execute a single SQL statement, this method acquires a connection from the the pool and executes the SQL
   * statement and returns it back after the execution.
   *
   * @param sql     the statement to execute
   * @param handler the result handler
   * @return self
   */
  @Fluent
  @Override
  default SQLClient query(String sql, Handler<AsyncResult<ResultSet>> handler) {
    getConnection(getConnection -> {
      if (getConnection.failed()) {
        handler.handle(Future.failedFuture(getConnection.cause()));
      } else {
        final SQLConnection conn = getConnection.result();
        conn.query(sql, query -> {
          if (query.failed()) {
            conn.close(close -> {
              if (close.failed()) {
                handler.handle(Future.failedFuture(close.cause()));
              } else {
                handler.handle(Future.failedFuture(query.cause()));
              }
            });
          } else {
            conn.close(close -> {
              if (close.failed()) {
                handler.handle(Future.failedFuture(close.cause()));
              } else {
                handler.handle(Future.succeededFuture(query.result()));
              }
            });
          }
        });
      }
    });
    return this;
  }

  /**
   * Executes the given SQL <code>SELECT</code> statement which returns the results of the query as a read stream.
   *
   * @param sql  the SQL to execute. For example <code>SELECT * FROM table ...</code>.
   * @param handler  the handler which is called once the operation completes. It will return a {@code SQLRowStream}.
   *
   * @see java.sql.Statement#executeQuery(String)
   * @see java.sql.PreparedStatement#executeQuery(String)
   */
  @Fluent
  @Override
  default SQLClient queryStream(String sql, Handler<AsyncResult<SQLRowStream>> handler) {
    getConnection(getConnection -> {
      if (getConnection.failed()) {
        handler.handle(Future.failedFuture(getConnection.cause()));
      } else {
        final SQLConnection conn = getConnection.result();
        conn.queryStream(sql, query -> {
          if (query.failed()) {
            conn.close(close -> {
              if (close.failed()) {
                handler.handle(Future.failedFuture(close.cause()));
              } else {
                handler.handle(Future.failedFuture(query.cause()));
              }
            });
          } else {
            SQLRowStream wrapped = new RowStreamWrapper(conn, query.result());
            handler.handle(Future.succeededFuture(wrapped));
          }
        });
      }
    });
    return this;
  }

  /**
   * Executes the given SQL <code>SELECT</code> statement which returns the results of the query as a read stream.
   *
   * @param sql  the SQL to execute. For example <code>SELECT * FROM table ...</code>.
   * @param params  these are the parameters to fill the statement.
   * @param handler  the handler which is called once the operation completes. It will return a {@code SQLRowStream}.
   *
   * @see java.sql.Statement#executeQuery(String)
   * @see java.sql.PreparedStatement#executeQuery(String)
   */
  @Fluent
  @Override
  default SQLClient queryStreamWithParams(String sql, JsonArray params, Handler<AsyncResult<SQLRowStream>> handler) {
    getConnection(getConnection -> {
      if (getConnection.failed()) {
        handler.handle(Future.failedFuture(getConnection.cause()));
      } else {
        final SQLConnection conn = getConnection.result();
        conn.queryStreamWithParams(sql, params, query -> {
          if (query.failed()) {
            conn.close(close -> {
              if (close.failed()) {
                handler.handle(Future.failedFuture(close.cause()));
              } else {
                handler.handle(Future.failedFuture(query.cause()));
              }
            });
          } else {
            SQLRowStream wrapped = new RowStreamWrapper(conn, query.result());
            handler.handle(Future.succeededFuture(wrapped));
          }
        });
      }
    });
    return this;
  }


  /**
   * Execute a single SQL prepared statement, this method acquires a connection from the the pool and executes the SQL
   * prepared statement and returns it back after the execution.
   *
   * @param sql       the statement to execute
   * @param arguments the arguments to the statement
   * @param handler   the result handler
   * @return self
   */
  @Fluent
  @Override
  default SQLClient queryWithParams(String sql, JsonArray arguments, Handler<AsyncResult<ResultSet>> handler) {
    getConnection(getConnection -> {
      if (getConnection.failed()) {
        handler.handle(Future.failedFuture(getConnection.cause()));
      } else {
        final SQLConnection conn = getConnection.result();
        conn.queryWithParams(sql, arguments, HandlerUtil.closeAndHandleResult(conn,handler));
      }
    });
    return this;
  }

  /**
   * Executes the given SQL statement which may be an <code>INSERT</code>, <code>UPDATE</code>, or <code>DELETE</code>
   * statement.
   *
   * @param sql  the SQL to execute. For example <code>INSERT INTO table ...</code>
   * @param handler  the handler which is called once the operation completes.
   *
   * @see java.sql.Statement#executeUpdate(String)
   * @see java.sql.PreparedStatement#executeUpdate(String)
   */
  @Fluent
  @Override
  default SQLClient update(String sql, Handler<AsyncResult<UpdateResult>> handler) {
    getConnection(getConnection -> {
      if (getConnection.failed()) {
        handler.handle(Future.failedFuture(getConnection.cause()));
      } else {
        final SQLConnection conn = getConnection.result();
        conn.update(sql, HandlerUtil.closeAndHandleResult(conn,handler));
      }
    });
    return this;
  }

  /**
   * Executes the given prepared statement which may be an <code>INSERT</code>, <code>UPDATE</code>, or <code>DELETE</code>
   * statement with the given parameters
   *
   * @param sql  the SQL to execute. For example <code>INSERT INTO table ...</code>
   * @param params  these are the parameters to fill the statement.
   * @param handler  the handler which is called once the operation completes.
   *
   * @see java.sql.Statement#executeUpdate(String)
   * @see java.sql.PreparedStatement#executeUpdate(String)
   */
  @Fluent
  @Override
  default SQLClient updateWithParams(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> handler) {
    getConnection(getConnection -> {
      if (getConnection.failed()) {
        handler.handle(Future.failedFuture(getConnection.cause()));
      } else {
        final SQLConnection conn = getConnection.result();
        conn.updateWithParams(sql, params, HandlerUtil.closeAndHandleResult(conn,handler));
      }
    });
    return this;
  }

  /**
   * Calls the given SQL <code>PROCEDURE</code> which returns the result from the procedure.
   *
   * @param sql  the SQL to execute. For example <code>{call getEmpName}</code>.
   * @param handler  the handler which is called once the operation completes. It will return a {@code ResultSet}.
   *
   * @see java.sql.CallableStatement#execute(String)
   */
  @Fluent
  @Override
  default SQLClient call(String sql, Handler<AsyncResult<ResultSet>> handler) {
    getConnection(getConnection -> {
      if (getConnection.failed()) {
        handler.handle(Future.failedFuture(getConnection.cause()));
      } else {
        final SQLConnection conn = getConnection.result();
        conn.call(sql, HandlerUtil.closeAndHandleResult(conn, handler));
      }
    });
    return this;
  }

  /**
   * Calls the given SQL <code>PROCEDURE</code> which returns the result from the procedure.
   *
   * The index of params and outputs are important for both arrays, for example when dealing with a prodecure that
   * takes the first 2 arguments as input values and the 3 arg as an output then the arrays should be like:
   *
   * <pre>
   *   params = [VALUE1, VALUE2, null]
   *   outputs = [null, null, "VARCHAR"]
   * </pre>
   *
   * @param sql  the SQL to execute. For example <code>{call getEmpName (?, ?)}</code>.
   * @param params  these are the parameters to fill the statement.
   * @param outputs  these are the outputs to fill the statement.
   * @param handler  the handler which is called once the operation completes. It will return a {@code ResultSet}.
   *
   * @see java.sql.CallableStatement#execute(String)
   */
  @Fluent
  @Override
  default SQLClient callWithParams(String sql, JsonArray params, JsonArray outputs, Handler<AsyncResult<ResultSet>> handler) {
    getConnection(getConnection -> {
      if (getConnection.failed()) {
        handler.handle(Future.failedFuture(getConnection.cause()));
      } else {
        final SQLConnection conn = getConnection.result();
        conn.callWithParams(sql, params, outputs, HandlerUtil.closeAndHandleResult(conn, handler));
      }
    });
    return this;
  }
}
