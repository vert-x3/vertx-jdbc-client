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
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLOptions;

import java.sql.*;
import java.sql.ResultSet;
import java.util.Collections;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class JDBCCallable extends AbstractJDBCAction<io.vertx.ext.sql.ResultSet> {

  private final String sql;
  private final JsonArray in;
  private final JsonArray out;

  public JDBCCallable(Vertx vertx, JDBCStatementHelper helper, SQLOptions options, ContextInternal ctx, String sql, JsonArray in, JsonArray out) {
    super(vertx, helper, options, ctx);
    this.sql = sql;
    this.in = in;
    this.out = out;
  }

  @Override
  public io.vertx.ext.sql.ResultSet execute(Connection conn) throws SQLException {
    try (CallableStatement statement = conn.prepareCall(sql)) {
      // apply statement options
      applyStatementOptions(statement);

      helper.fillStatement(statement, in, out);

      boolean retResult = statement.execute();
      boolean outResult = out != null && out.size() > 0;

      io.vertx.ext.sql.ResultSet resultSet = null;

      if (retResult) {
        io.vertx.ext.sql.ResultSet ref = null;
        // normal return only
        while (retResult) {
          try (ResultSet rs = statement.getResultSet()) {
            // 1st rs
            if (ref == null) {
              resultSet = helper.asList(rs);
              ref = resultSet;
            } else {
              ref.setNext(helper.asList(rs));
              ref = ref.getNext();
            }
            if (outResult) {
              // add the registered outputs
              ref.setOutput(convertOutputs(statement));
            }
          }
          retResult = statement.getMoreResults();
        }
      } else {
        if (outResult) {
          // only outputs are available
          resultSet = new io.vertx.ext.sql.ResultSet(Collections.emptyList(), Collections.emptyList(), null).setOutput(convertOutputs(statement));
        }
      }

      // no return
      return resultSet;
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
          result.add(helper.asList((ResultSet) value));
        } else {
          result.add(JDBCStatementHelper.convertSqlValue(value));
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
