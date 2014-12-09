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

package io.vertx.ext.jdbc.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.jdbc.JdbcTransaction;
import io.vertx.ext.jdbc.impl.actions.JdbcCommit;
import io.vertx.ext.jdbc.impl.actions.JdbcRollback;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
class JdbcTransactionImpl extends JdbcActionsImpl implements JdbcTransaction {

  private static final Logger log = LoggerFactory.getLogger(JdbcTransactionImpl.class);

  private final long timeout;
  private long timerId;
  private volatile boolean resetting;

  private Handler<Long> timeoutHandler = id -> {
    if (!resetting) {
      log.error("Transaction timed out. Attempting to rollback");
      rollback(ar -> {
        if (ar.succeeded()) {
          log.error("Successfully rolled back timed out transaction.");
        } else {
          log.error("Failed to roll back timed out transaction.", ar.cause());
        }
      });
    }
  };

  JdbcTransactionImpl(Vertx vertx, Connection conn, long timeout) throws SQLException {
    this(vertx, conn, -1, timeout);
  }

  JdbcTransactionImpl(Vertx vertx, Connection conn, int isolation, long timeout) throws SQLException {
    super(vertx, conn);
    conn.setAutoCommit(false);
    if (isolation != -1) {
      conn.setTransactionIsolation(isolation);
    }
    this.timeout = timeout;
    timerId = vertx.setTimer(timeout, timeoutHandler);
  }

  @Override
  public void execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
    resetTimer();
    super.execute(sql, resultHandler);
  }

  @Override
  public void query(String sql, JsonArray params, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
    resetTimer();
    super.query(sql, params, resultHandler);
  }

  @Override
  public void update(String sql, JsonArray params, Handler<AsyncResult<JsonObject>> resultHandler) {
    resetTimer();
    super.update(sql, params, resultHandler);
  }

  @Override
  public void commit(Handler<AsyncResult<Void>> handler) {
    vertx.cancelTimer(timerId);
    new JdbcCommit(vertx, conn).process(handler);
  }

  @Override
  public void rollback(Handler<AsyncResult<Void>> handler) {
    vertx.cancelTimer(timerId);
    new JdbcRollback(vertx, conn).process(handler);
  }

  private void resetTimer() {
    resetting = true;
    vertx.cancelTimer(timerId);
    timerId = vertx.setTimer(timeout, timeoutHandler);
    resetting = false;
  }
}
