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

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper.fillStatement;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class JDBCBatch extends AbstractJDBCAction<List<Integer>> {

  enum Type {
    STATEMENT,
    PREPARED,
    CALLABLE
  }

  private final Type type;
  private final List<String> sql;
  private final List<JsonArray> in;
  private final List<JsonArray> out;

  public JDBCBatch(Vertx vertx, Connection connection, Context context, List<String> sql) {
    this(vertx, connection, context, Type.STATEMENT, sql, null, null);
  }

  public JDBCBatch(Vertx vertx, Connection connection, Context context, String sql, List<JsonArray> in) {
    this(vertx, connection, context, Type.PREPARED, Collections.singletonList(sql), in, null);
  }

  public JDBCBatch(Vertx vertx, Connection connection, Context context, String sql, List<JsonArray> in, List<JsonArray> out) {
    this(vertx, connection, context, Type.CALLABLE, Collections.singletonList(sql), in, out);
  }

  private JDBCBatch(Vertx vertx, Connection connection, Context context, Type type, List<String> sql, List<JsonArray> in, List<JsonArray> out) {
    super(vertx, connection, context);
    this.type = type;
    this.sql = sql;
    this.in = in;
    this.out = out;
  }

  @Override
  protected List<Integer> execute() throws SQLException {
    final int[] result;

    switch (type) {
      case STATEMENT:
        try (Statement stmt = conn.createStatement()) {
          for (String query : sql) {
            stmt.addBatch(query);
          }

          result = stmt.executeBatch();
        }
        break;
      case PREPARED:
        try (PreparedStatement stmt = conn.prepareStatement(sql.get(0))) {
          for (JsonArray in : this.in) {
            fillStatement(stmt, in);
            stmt.addBatch();
          }

          result = stmt.executeBatch();
        }
        break;
      case CALLABLE:
        try (CallableStatement stmt = conn.prepareCall(sql.get(0))) {
          final int max_in = in.size();
          final int max_out = out.size();

          for (int i = 0; i < Math.max(max_in, max_out); i++) {
            final JsonArray jin = i < max_in ? in.get(i) : null;
            final JsonArray jout = i < max_out ? out.get(i) : null;
            fillStatement(stmt, jin, jout);
            stmt.addBatch();
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

  @Override
  protected String name() {
    return "batch";
  }
}
