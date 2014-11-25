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

package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.jdbc.RuntimeSqlException;
import io.vertx.ext.jdbc.impl.Transactions;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public abstract class AbstractJdbcAction<T> {

  private static final Logger log = LoggerFactory.getLogger(AbstractJdbcAction.class);

  protected final Vertx vertx;
  protected final Connection conn;
  protected String txId;

  public AbstractJdbcAction(Vertx vertx, DataSource dataSource) {
    this(vertx, connection(dataSource));
  }

  public AbstractJdbcAction(Vertx vertx, Transactions transactions, String txId) {
    this(vertx, transactions.get(txId), txId);
  }

  public AbstractJdbcAction(Vertx vertx, Connection conn) {
    this(vertx, conn, null);
  }

  private AbstractJdbcAction(Vertx vertx, Connection conn, String txId) {
    this.vertx = vertx;
    this.conn = conn;
    this.txId = txId;
  }

  public void process(Handler<AsyncResult<T>> resultHandler) {
    T result = null;
    Throwable cause = null;

    // If transaction and no active connection
    if (txId != null && conn == null) {
      handleError(new SQLException("No active connection for transaction " + txId), resultHandler);
      return;
    }

    // Execute
    try {
      result = execute(conn);
    } catch (Throwable t) {
      cause = t;
      // If transaction, rollback
      if (txId != null) {
        rollback(conn, txId);
        txId = null;
      }
    } finally {
      // If not transaction, close (return to pool)
      if (txId == null) {
        close(conn);
      }
    }

    if (cause == null) {
      handleSuccess(result, resultHandler);
    } else {
      handleError(cause, resultHandler);
    }
  }

  protected abstract T execute(Connection conn) throws SQLException;

  protected abstract String name();


  protected void handleSuccess(T result, Handler<AsyncResult<T>> resultHandler) {
    resultHandler.handle(Future.completedFuture(result));
  }

  protected void handleError(Throwable t, Handler<AsyncResult<T>> resultHandler) {
    // Log the message at warn, so ppl can turn off. This is nice when you want a full
    // stacktrace which you lose over the bus
    log.warn("Exception occurred executing JDBC Service action " + name(), t);
    resultHandler.handle(Future.completedFuture(t));
  }

  private static void close(Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  protected static void close(ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  private static void rollback(Connection conn, String txId) {
    if (conn != null) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        log.error("Exception attempting rollback for transaction " + txId);
      }
    }
  }

  protected static Connection connection(DataSource dataSource) {
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }
  }
}
