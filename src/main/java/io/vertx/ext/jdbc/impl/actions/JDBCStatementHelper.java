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

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
final class JDBCStatementHelper {

  private static final JsonArray EMPTY = new JsonArray(Collections.unmodifiableList(new ArrayList<>()));

  private static final Pattern DATETIME = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$");
  private static final Pattern DATE = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}$");
  private static final Pattern TIME = Pattern.compile("^\\d{2}:\\d{2}:\\d{2}$");
  private static final Pattern UUID = Pattern.compile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");

  private JDBCStatementHelper() {}

  public static void fillStatement(PreparedStatement statement, JsonArray in) throws SQLException {
    if (in == null) {
      in = EMPTY;
    }

    for (int i = 0; i < in.size(); i++) {
      Object value = in.getValue(i);

      if (value != null) {
        if (value instanceof String) {
          statement.setObject(i + 1, optimisticCast((String) value));
        } else {
          statement.setObject(i + 1, value);
        }
      } else {
        statement.setNull(i + 1, Types.NULL);
      }
    }
  }

  public static void fillStatement(CallableStatement statement, JsonArray in, JsonArray out) throws SQLException {
    if (in == null) {
      in = EMPTY;
    }

    if (out == null) {
      out = EMPTY;
    }

    int max = Math.max(in.size(), out.size());

    for (int i = 0; i < max; i++) {
      Object value = null;
      boolean set = false;

      if (i < in.size()) {
        value = in.getValue(i);
      }

      // found a in value, use it as a input parameter
      if (value != null) {
        if (value instanceof String) {
          statement.setObject(i + 1, optimisticCast((String) value));
        } else {
          statement.setObject(i + 1, value);
        }
        set = true;
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
        statement.registerOutParameter(i + 1, JDBCType.valueOf((String) value).getVendorTypeNumber());
        set = true;
      }

      if (!set) {
        // assume null input
        statement.setNull(i + 1, Types.NULL);
      }
    }
  }

  public static io.vertx.ext.sql.ResultSet asList(ResultSet rs) throws SQLException {

    List<String> columnNames = new ArrayList<>();
    ResultSetMetaData metaData = rs.getMetaData();
    int cols = metaData.getColumnCount();
    for (int i = 1; i <= cols; i++) {
      columnNames.add(metaData.getColumnLabel(i));
    }

    List<JsonArray> results = new ArrayList<>();

    while (rs.next()) {
      JsonArray result = new JsonArray();
      for (int i = 1; i <= cols; i++) {
        Object res = convertSqlValue(rs.getObject(i));
        if (res != null) {
          result.add(res);
        } else {
          result.addNull();
        }
      }
      results.add(result);
    }

    return new io.vertx.ext.sql.ResultSet(columnNames, results);
  }

  public static Object convertSqlValue(Object value) {
    if (value == null) {
      return null;
    }

    // valid json types are just returned as is
    if (value instanceof Boolean || value instanceof String || value instanceof byte[]) {
      return value;
    }

    // numeric values
    if (value instanceof Number) {
      if (value instanceof BigDecimal) {
        BigDecimal d = (BigDecimal) value;
        if (d.scale() == 0) {
          return ((BigDecimal) value).toBigInteger();
        } else {
          // we might loose precision here
          return ((BigDecimal) value).doubleValue();
        }
      }

      return value;
    }

    // temporal values
    if (value instanceof Date || value instanceof Time || value instanceof Timestamp) {
      return OffsetDateTime.ofInstant(Instant.ofEpochMilli(((java.util.Date) value).getTime()), ZoneOffset.UTC).format(ISO_OFFSET_DATE_TIME);
    }

    // large objects
    if (value instanceof Clob) {
      Clob c = (Clob) value;
      try {
        // result might be truncated due to downcasting to int
        String tmp = c.getSubString(1, (int) c.length());
        c.free();

        return tmp;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    if (value instanceof Blob) {
      Blob b = (Blob) value;
      try {
        // result might be truncated due to downcasting to int
        byte[] tmp = b.getBytes(1, (int) b.length());
        b.free();
        return tmp;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    // arrays
    if (value instanceof Array) {
      Array a = (Array) value;
      try {
        Object[] arr = (Object[]) a.getArray();
        if (arr != null) {
          JsonArray jsonArray = new JsonArray();
          for (Object o : arr) {
            jsonArray.add(convertSqlValue(o));
          }

          a.free();

          return jsonArray;
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    // fallback to String
    return value.toString();
  }

  private static Object optimisticCast(String value) {
    if (value == null) {
      return null;
    }

    // sql time
    if (TIME.matcher(value).matches()) {
      return new java.sql.Time(Instant.from(ISO_INSTANT.parse("1900-01-01T" + value)).toEpochMilli());
    }

    // sql date
    if (DATE.matcher(value).matches()) {
      return new java.sql.Date(Instant.from(ISO_INSTANT.parse(value + "T00:00:00Z")).toEpochMilli());
    }

    // sql timestamp
    if (DATETIME.matcher(value).matches()) {
      return new java.sql.Timestamp(Instant.from(ISO_INSTANT.parse(value)).toEpochMilli());
    }

    // sql uuid
    if (UUID.matcher(value).matches()) {
      return java.util.UUID.fromString(value);
    }

    // unknown
    return value;
  }
}
