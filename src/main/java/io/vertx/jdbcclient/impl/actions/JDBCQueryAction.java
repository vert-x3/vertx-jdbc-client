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

import io.vertx.core.json.JsonArray;
import io.vertx.ext.jdbc.impl.actions.AbstractJDBCAction;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.jdbcclient.impl.JDBCRow;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.RowDesc;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public abstract class JDBCQueryAction<C, R> extends AbstractJDBCAction<JDBCQueryAction.Response<R>> {

  private final Collector<Row, C, R> collector;

  public JDBCQueryAction(JDBCStatementHelper helper, SQLOptions options, Collector<Row, C, R> collector) {
    super(helper, options);
    this.collector = collector;
  }

  public static class Response<R> {
    public final R result;
    public final RowDesc rowDesc;
    public final int size;
    public Response(R result, RowDesc rowDesc, int size) {
      this.result = result;
      this.rowDesc = rowDesc;
      this.size = size;
    }
  }

  protected Response<R> decode(Statement statement) throws SQLException {
    BiConsumer<C, Row> accumulator = collector.accumulator();
    List<String> columnNames = new ArrayList<>();
    ResultSet rs = statement.getResultSet();
    RowDesc desc = new RowDesc(columnNames);
    C container = collector.supplier().get();
    int size = 0;
    if (rs != null) {
      ResultSetMetaData metaData = rs.getMetaData();
      int cols = metaData.getColumnCount();
      for (int i = 1; i <= cols; i++) {
        columnNames.add(metaData.getColumnLabel(i));
      }
      while (rs.next()) {
        size++;
        Row row = new JDBCRow(desc);
        for (int i = 1; i <= cols; i++) {
          Object res = convertSqlValue(rs.getObject(i));
          row.addValue(res);
        }
        accumulator.accept(container, row);
      }
    }
    R result = collector.finisher().apply(container);
    return new Response<>(result, desc, size);
  }

  public static Object convertSqlValue(Object value) throws SQLException {
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
    if (value instanceof Time) {
      return ((Time) value).toLocalTime().atOffset(ZoneOffset.UTC).format(ISO_LOCAL_TIME);
    }

    if (value instanceof Date) {
      return ((Date) value).toLocalDate();
    }

    if (value instanceof Timestamp) {
      return OffsetDateTime.ofInstant(((Timestamp) value).toInstant(), ZoneOffset.UTC).format(ISO_OFFSET_DATE_TIME);
    }

    // large objects
    if (value instanceof Clob) {
      Clob c = (Clob) value;
      try {
        // result might be truncated due to downcasting to int
        return c.getSubString(1, (int) c.length());
      } finally {
        try {
          c.free();
        } catch (AbstractMethodError | SQLFeatureNotSupportedException e) {
          // ignore since it is an optional feature since 1.6 and non existing before 1.6
        }
      }
    }

    if (value instanceof Blob) {
      Blob b = (Blob) value;
      try {
        // result might be truncated due to downcasting to int
        return b.getBytes(1, (int) b.length());
      } finally {
        try {
          b.free();
        } catch (AbstractMethodError | SQLFeatureNotSupportedException e) {
          // ignore since it is an optional feature since 1.6 and non existing before 1.6
        }
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
          return jsonArray;
        }
      } finally {
        a.free();
      }
    }

    // fallback to String
    return value.toString();
  }
}
