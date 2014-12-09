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

  protected AbstractJdbcAction(Vertx vertx, Connection conn) {
    this.vertx = vertx;
    this.conn = conn;
  }

  public void process(Handler<AsyncResult<T>> resultHandler) {
    T result = null;
    Throwable cause = null;

    // Execute
    try {
      result = execute(conn);
    } catch (Throwable t) {
      cause = t;
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
    resultHandler.handle(Future.succeededFuture(result));
  }

  protected void handleError(Throwable t, Handler<AsyncResult<T>> resultHandler) {
    // Log the message at warn, so ppl can turn off. This is nice when you want a full
    // stacktrace which you lose over the bus
    log.warn("Exception occurred executing JDBC Service action " + name(), t);
    resultHandler.handle(Future.failedFuture(t));
  }

  protected static void close(Connection conn) {
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
}
