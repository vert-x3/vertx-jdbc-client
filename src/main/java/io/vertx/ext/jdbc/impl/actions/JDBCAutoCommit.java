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

import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.ext.sql.SQLOptions;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCAutoCommit extends AbstractJDBCAction<Void> {
  private boolean autoCommit;

  public JDBCAutoCommit(Vertx vertx, SQLOptions options, ContextInternal ctx, boolean autoCommit) {
    super(vertx, options, ctx);
    this.autoCommit = autoCommit;
  }

  @Override
  public Void execute(Connection conn) throws SQLException {
    conn.setAutoCommit(autoCommit);
    // setting the auto commit to false triggers a new transaction to begin
    if (!autoCommit) {
      if (options != null && options.getTransactionIsolation() != null) {
        conn.setTransactionIsolation(options.getTransactionIsolation().getType());
      }
    }
    return null;
  }

  @Override
  protected String name() {
    return "autoCommit";
  }
}
