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
import io.vertx.core.WorkerExecutor;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public abstract class AbstractJDBCAction<T> {

  private static final Logger log = LoggerFactory.getLogger(AbstractJDBCAction.class);

  protected final Vertx vertx;
  protected final Connection conn;
  protected final WorkerExecutor exec;

  protected AbstractJDBCAction(Vertx vertx, Connection conn, WorkerExecutor exec) {
    this.vertx = vertx;
    this.conn = conn;
    this.exec = exec;
  }

  public void handle(Future<T> future) {
    try {
      T result = execute();
      future.complete(result);
    } catch (SQLException e) {
      future.fail(e);
    }
  }

  public void execute(Handler<AsyncResult<T>> resultHandler) {
    exec.executeBlocking(this::handle, resultHandler);
  }

  protected abstract T execute() throws SQLException;

  protected abstract String name();
}
