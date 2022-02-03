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
import io.vertx.ext.jdbc.spi.JDBCColumnDescriptorProvider;
import io.vertx.ext.sql.SQLOptions;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class JDBCBatch extends AbstractJDBCAction<List<Integer>> {

  private enum Type {
    STATEMENT,
    PREPARED,
    CALLABLE
  }

  private final Type type;
  private final List<String> sql;
  private final List<JsonArray> in;
  private final List<JsonArray> out;

  public JDBCBatch(JDBCStatementHelper helper, SQLOptions options, List<String> sql) {
    this(helper, options, Type.STATEMENT, sql, null, null);
  }

  public JDBCBatch(JDBCStatementHelper helper, SQLOptions options, String sql, List<JsonArray> in) {
    this(helper, options, Type.PREPARED, Collections.singletonList(sql), in, null);
  }

  public JDBCBatch(JDBCStatementHelper helper, SQLOptions options, String sql, List<JsonArray> in, List<JsonArray> out) {
    this(helper, options, Type.CALLABLE, Collections.singletonList(sql), in, out);
  }

  private JDBCBatch(JDBCStatementHelper helper, SQLOptions options, Type type, List<String> sql, List<JsonArray> in, List<JsonArray> out) {
    super(helper, options);
    this.type = type;
    this.sql = sql;
    this.in = in;
    this.out = out;
  }

  @Override
  public List<Integer> execute(Connection conn) throws SQLException {
    final int[] result;

    switch (type) {
      case STATEMENT:
        try (Statement stmt = conn.createStatement()) {
          // apply statement options
          applyStatementOptions(stmt);

          for (String query : sql) {
            stmt.addBatch(query);
          }

          result = stmt.executeBatch();
        }
        break;
      case PREPARED:
        try (PreparedStatement stmt = conn.prepareStatement(sql.get(0))) {
          // apply statement options
          applyStatementOptions(stmt);

          if (!this.in.isEmpty()) {
            for (JsonArray in : this.in) {
              JDBCColumnDescriptorProvider provider = JDBCColumnDescriptorProvider.fromParameterMetaData(stmt.getParameterMetaData());
              fillStatement(stmt, in, provider);
              stmt.addBatch();
            }
          }

          result = stmt.executeBatch();
        }
        break;
      case CALLABLE:
        try (CallableStatement stmt = conn.prepareCall(sql.get(0))) {
          // apply statement options
          applyStatementOptions(stmt);

          final int max_in = in.size();
          final int max_out = out.size();
          final int max_in_out = Math.max(max_in, max_out);

          if (max_in_out > 0) {
            for (int i = 0; i < max_in_out; i++) {
              JDBCColumnDescriptorProvider provider = JDBCColumnDescriptorProvider.fromParameterMetaData(stmt.getParameterMetaData());
              final JsonArray jin = i < max_in ? in.get(i) : null;
              final JsonArray jout = i < max_out ? out.get(i) : null;
              fillStatement(stmt, jin, jout, provider);
              stmt.addBatch();
            }
          }

          result = stmt.executeBatch();
        }
        break;
      default:
        return Collections.emptyList();
    }

    final List<Integer> list = new ArrayList<>(result.length);

    for (int res : result) {
      list.add(res);
    }

    return list;
  }

}
