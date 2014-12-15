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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JdbcQuery extends AbstractJdbcStatement<List<JsonObject>> {

  public JdbcQuery(Vertx vertx, Connection connection, String sql, JsonArray parameters) {
    super(vertx, connection, sql, parameters);
  }

  @Override
  protected List<JsonObject> executeStatement(PreparedStatement statement) throws SQLException {
    ResultSet rs = statement.executeQuery();
    List<JsonObject> results = asList(rs);

    safeClose(rs);

    return results;
  }

  @Override
  protected String name() {
    return "executeQuery";
  }
}
