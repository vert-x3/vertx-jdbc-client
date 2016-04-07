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
import java.sql.ResultSet;

import static io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper.*;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCQuery extends AbstractJDBCAction<io.vertx.ext.sql.ResultSet> {

  private final String sql;
  private final JsonArray in;

  public JDBCQuery(Vertx vertx, Connection connection, Context context, String sql, JsonArray in) {
    super(vertx, connection, context);
    this.sql = sql;
    this.in = in;
  }

  @Override
  protected io.vertx.ext.sql.ResultSet execute() throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement(sql)) {
      fillStatement(statement, in);

      try (ResultSet rs = statement.executeQuery()) {
        return asList(rs);
      }
    }
  }

  @Override
  protected String name() {
    return "query";
  }
}
