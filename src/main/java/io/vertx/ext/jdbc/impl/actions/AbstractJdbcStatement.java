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
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public abstract class AbstractJdbcStatement<T> extends AbstractJdbcAction<T> {
  private final String sql;
  private final JsonArray parameters;

  protected AbstractJdbcStatement(Vertx vertx, Connection connection, String sql, JsonArray parameters) {
    super(vertx, connection);
    this.sql = sql;
    this.parameters = parameters;
  }

  @Override
  protected final T execute(Connection conn) throws SQLException {
    try (PreparedStatement statement = preparedStatement(conn, sql)) {
      fillStatement(statement, parameters);

      return executeStatement(statement);
    }
  }

  protected PreparedStatement preparedStatement(Connection conn, String sql) throws SQLException {
    return conn.prepareStatement(sql);
  }

  protected abstract T executeStatement(PreparedStatement statement) throws SQLException;

  protected void fillStatement(PreparedStatement statement, JsonArray parameters) throws SQLException {
    if (parameters == null || parameters.size() == 0) {
      return;
    }
    for (int i = 0; i < parameters.size(); i++) {
      statement.setObject(i + 1, parameters.getValue(i));
    }
  }

  protected io.vertx.ext.sql.ResultSet asList(ResultSet rs) throws SQLException {

    List<String> columnNames = new ArrayList<>();
    ResultSetMetaData metaData = rs.getMetaData();
    int cols = metaData.getColumnCount();
    for (int i = 1; i <= cols; i++) {
      columnNames.add(metaData.getColumnName(i));
    }

    List<JsonArray> results = new ArrayList<>();

    while (rs.next()) {
      JsonArray result = new JsonArray();
      for (int i = 1; i <= cols; i++) {
        result.add(convertSqlValue(rs.getObject(i)));
      }
      results.add(result);
    }

    return new io.vertx.ext.sql.ResultSet(columnNames, results);
  }

  protected Object convertSqlValue(Object value) {
    if (value instanceof Date || value instanceof Time || value instanceof Timestamp) {
      return value.toString();
    } else {
      return value;
    }
  }
}
