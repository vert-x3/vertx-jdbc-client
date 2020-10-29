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

import io.vertx.ext.sql.SQLOptions;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public abstract class AbstractJDBCAction<T> {

  protected final SQLOptions options;
  protected final JDBCStatementHelper helper;

  protected AbstractJDBCAction(SQLOptions options) {
    this(null, options);
  }

  protected AbstractJDBCAction(JDBCStatementHelper helper, SQLOptions options) {
    this.options = options;
    this.helper = helper;
  }

  public abstract T execute(Connection conn) throws SQLException;

  protected void applyStatementOptions(Statement statement) throws SQLException {
    if (options != null) {
      if (options.getQueryTimeout() > 0) {
        statement.setQueryTimeout(options.getQueryTimeout());
      }
      if (options.getFetchDirection() != null) {
        statement.setFetchDirection(options.getFetchDirection().getType());
      }
      if (options.getFetchSize() > 0) {
        statement.setFetchSize(options.getFetchSize());
      }
      if (options.getMaxRows() > 0) {
        statement.setMaxRows(options.getMaxRows());
      }
    }
  }

}
