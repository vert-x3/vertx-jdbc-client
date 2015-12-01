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
import io.vertx.core.json.JsonArray;

import java.sql.*;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper.*;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class JDBCCallable extends AbstractJDBCAction<io.vertx.ext.sql.ResultSet> {

  private final String sql;
  private final JsonArray in;
  private final JsonArray out;

  public JDBCCallable(Vertx vertx, Connection connection, String sql, JsonArray in, JsonArray out) {
    super(vertx, connection);
    this.sql = sql;
    this.in = in;
    this.out = out;
  }

  @Override
  protected io.vertx.ext.sql.ResultSet execute(Connection conn) throws SQLException {
    try (CallableStatement statement = conn.prepareCall(sql)) {
      fillStatement(statement, in, out);

      boolean retResult = statement.execute();
      boolean outResult = out != null && out.size() > 0;

      if (retResult) {
        // normal return only
        try (ResultSet rs = statement.getResultSet()) {
          if (outResult) {
            // add the registered outputs
            return asList(rs).setOutput(convertOutputs(statement));
          } else {
            return asList(rs);
          }
        }
      } else {
        if (outResult) {
          // only outputs are available
          return new io.vertx.ext.sql.ResultSet(Collections.emptyList(), Collections.emptyList()).setOutput(convertOutputs(statement));
        }
      }

      // no return
      return null;
    }
  }

  private JsonArray convertOutputs(CallableStatement statement) throws SQLException {
    JsonArray result = new JsonArray();

    for (int i = 0; i < out.size(); i++) {
      Object var = out.getValue(i);

      if (var != null) {
        Object value = statement.getObject(i + 1);
        if (value == null) {
          result.addNull();
        } else if (value instanceof ResultSet) {
          result.add(asList((ResultSet) value));
        } else {
          result.add(convertSqlValue(value));
        }
      } else {
        result.addNull();
      }
    }

    return result;
  }

  @Override
  protected String name() {
    return "callable";
  }
}
