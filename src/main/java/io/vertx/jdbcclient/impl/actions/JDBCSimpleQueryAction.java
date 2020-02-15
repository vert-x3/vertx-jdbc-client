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

package io.vertx.jdbcclient.impl.actions;

import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.sqlclient.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collector;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCSimpleQueryAction<C, R> extends JDBCQueryAction<C, R> {

  private final String sql;

  public JDBCSimpleQueryAction(JDBCStatementHelper helper, SQLOptions options, String sql, Collector<Row, C, R> collector) {
    super(helper, options, collector);
    this.sql = sql;
  }

  @Override
  public Response<R> execute(Connection conn) throws SQLException {
    try (Statement statement = conn.createStatement()) {
      // apply statement options
      applyStatementOptions(statement);
      statement.execute(sql);
      return decode(statement);
    }
  }
}
