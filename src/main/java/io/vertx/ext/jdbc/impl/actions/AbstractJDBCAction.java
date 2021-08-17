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

import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLOptions;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public abstract class AbstractJDBCAction<T> {

  protected final SQLOptions options;
  protected final JDBCStatementHelper helper;
  private static final JsonArray EMPTY = new JsonArray(Collections.unmodifiableList(new ArrayList<>()));

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
      if (options.getFetchSize() != 0) {
        statement.setFetchSize(options.getFetchSize());
      }
      if (options.getMaxRows() > 0) {
        statement.setMaxRows(options.getMaxRows());
      }
    }
  }

  protected io.vertx.ext.sql.ResultSet asList(ResultSet rs) throws SQLException {

    List<String> columnNames = new ArrayList<>();
    JDBCTypeProvider provider = JDBCTypeProvider.fromResult(rs);
    ResultSetMetaData metaData = rs.getMetaData();
    int cols = metaData.getColumnCount();
    for (int i = 1; i <= cols; i++) {
      columnNames.add(metaData.getColumnLabel(i));
    }

    List<JsonArray> results = new ArrayList<>();

    while (rs.next()) {
      JsonArray result = new JsonArray();
      for (int i = 1; i <= cols; i++) {
        Object res = helper.getDecoder().parse(rs, i, provider);
        if (res != null) {
          result.add(res);
        } else {
          result.addNull();
        }
      }
      results.add(result);
    }

    return new io.vertx.ext.sql.ResultSet(columnNames, results, null);
  }

  protected void fillStatement(PreparedStatement statement, JsonArray in) throws SQLException {
    if (in == null) {
      in = EMPTY;
    }

    JDBCTypeProvider provider = JDBCTypeProvider.fromParameter(statement);
    for (int pos = 1; pos <= in.size(); pos++) {
      statement.setObject(pos, helper.getEncoder().encode(in, pos, provider));
    }
  }

  protected void fillStatement(CallableStatement statement, JsonArray in, JsonArray out) throws SQLException {
    if (in == null) {
      in = EMPTY;
    }

    if (out == null) {
      out = EMPTY;
    }

    int max = Math.max(in.size(), out.size());

    JDBCTypeProvider provider = JDBCTypeProvider.fromParameter(statement);
    for (int i = 0; i < max; i++) {
      Object value;
      boolean set = false;

      if (i < in.size()) {
        value = helper.getEncoder().encode(in, i + 1, provider);
        if (value != null) {
          statement.setObject(i + 1, value);
          set = true;
        }
      }

      // reset
      value = null;

      if (i < out.size()) {
        value = out.getValue(i);
      }

      // found a out value, use it as a output parameter
      if (value != null) {
        // We're using the int from the enum instead of the enum itself to allow working with Drivers
        // that have not been upgraded to Java8 yet.
        if (value instanceof String) {
          statement.registerOutParameter(i + 1, JDBCType.valueOf((String) value).getVendorTypeNumber());
        } else if (value instanceof Number) {
          // for cases where vendors have special codes (e.g.: Oracle)
          statement.registerOutParameter(i + 1, ((Number) value).intValue());
        }
        set = true;
      }

      if (!set) {
        // assume null input
        statement.setNull(i + 1, Types.NULL);
      }
    }
  }
}
