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
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class StreamQuery extends AbstractJDBCAction<SQLRowStream> {

  private final String sql;
  private final JsonArray in;
  private final int timeout;
  private final int rowStreamFetchSize;

  public StreamQuery(Vertx vertx, JDBCStatementHelper helper, Connection connection, WorkerExecutor exec, int timeout, int rowStreamFetchSize, String sql, JsonArray in) {
    super(vertx, helper, connection, exec);
    this.sql = sql;
    this.in = in;
    this.timeout = timeout;
    this.rowStreamFetchSize = rowStreamFetchSize;
  }

  @Override
  protected SQLRowStream execute() throws SQLException {
    PreparedStatement st = null;

    try {
      st = conn.prepareStatement(sql);

      if (timeout >= 0) {
        st.setQueryTimeout(timeout);
      }

      helper.fillStatement(st, in);
      ResultSet rs = null;

      try {
        rs = st.executeQuery();
        return new JDBCSQLRowStream(exec, st, rs, rowStreamFetchSize);
      } catch (SQLException e) {
        if (rs != null) {
          rs.close();
        }
        throw e;
      }
    } catch (SQLException e) {
      if (st != null) {
        st.close();
      }
      throw e;
    }
  }

  @Override
  protected String name() {
    return "stream";
  }
}
