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
import io.vertx.sqlclient.Tuple;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.stream.Collector;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCPreparedQuery<C, R> extends JDBCQueryAction<C, R> {

  private final String sql;
  private final PreparedStatement statement;
  private final Tuple params;

  public JDBCPreparedQuery(JDBCStatementHelper helper, SQLOptions options, PreparedStatement statement, String sql, Collector<Row, C, R> collector, Tuple params) {
    super(helper, options, collector);
    this.sql = sql;
    this.statement = statement;
    this.params = params;
  }

  @Override
  public Response<R> execute(Connection conn) throws SQLException {
    fillStatement(statement, params);
    statement.execute();
    return decode(statement);
  }

  public void fillStatement(PreparedStatement statement, Tuple tuple) throws SQLException {
    for (int i = 0; i < tuple.size(); i++) {
      Object value = tuple.getValue(i);
      if (value instanceof LocalDate) {
        LocalDate date = (LocalDate) value;
        value = Date.valueOf(date);
      }
      statement.setObject(i + 1, value);
    }
  }
}
