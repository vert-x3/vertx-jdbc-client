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
import io.vertx.ext.jdbc.impl.Transactions;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public abstract class AbstractJdbcUpdate<T> extends AbstractJdbcStatement<T> {

  public AbstractJdbcUpdate(Vertx vertx, DataSource dataSource, String sql, JsonArray parameters) {
    super(vertx, dataSource, sql, parameters);
  }

  public AbstractJdbcUpdate(Vertx vertx, Transactions transactions, String txId, String sql, JsonArray parameters) {
    super(vertx, transactions, txId, sql, parameters);
  }

  @Override
  protected T executeStatement(PreparedStatement statement) throws SQLException {
    return parseUpdate(statement, statement.executeUpdate());
  }

  protected abstract T parseUpdate(PreparedStatement statement, int updated) throws SQLException;
}
