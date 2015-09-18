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
import io.vertx.ext.sql.UpdateResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCUpdate extends AbstractJDBCStatement<UpdateResult> {

  public JDBCUpdate(Vertx vertx, Connection connection, String sql, JsonArray parameters) {
    super(vertx, connection, sql, parameters);
  }

  @Override
  protected PreparedStatement preparedStatement(Connection conn, String sql) throws SQLException {
    if ( hasNamedParameters() ) {
      String sqlReplaced = parseForNamedParameters();
      return conn.prepareStatement(sqlReplaced, Statement.RETURN_GENERATED_KEYS);
    } else {
      return conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
    }
  }

  @Override
  protected UpdateResult executeStatement(PreparedStatement statement) throws SQLException {
    int updated = statement.executeUpdate();
    JsonObject result = new JsonObject();
    result.put("updated", updated);
    // Create JsonArray of keys
    ResultSet rs = statement.getGeneratedKeys();
    JsonArray keys = new JsonArray();
    while (rs.next()) {
      keys.add(convertSqlValue(rs.getObject(1)));
    }
    rs.close();
    return new UpdateResult(updated, keys);
  }

  @Override
  protected String name() {
    return "update";
  }
}
