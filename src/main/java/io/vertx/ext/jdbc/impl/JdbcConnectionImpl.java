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
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.jdbc.JdbcConnection;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
class JdbcConnectionImpl extends JdbcActionsImpl implements JdbcConnection {

  public JdbcConnectionImpl(Vertx vertx, Connection conn) {
    super(vertx, conn);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    try {
      conn.close();
      handler.handle(Future.succeededFuture());
    } catch (SQLException e) {
      handler.handle(Future.failedFuture(e));
    }
  }
}
