/*
 * Copyright (c) 2011-2026 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.jdbcclient.impl.actions;

import io.vertx.jdbcclient.SqlOptions;
import io.vertx.sqlclient.spi.protocol.SavepointCommand;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Map;

public class JDBCSavepointOp<R> extends AbstractJDBCAction<R> {

  private final SavepointCommand<R> op;
  private final Map<String, Savepoint> savepoints;

  public JDBCSavepointOp(JDBCStatementHelper helper, SavepointCommand<R> op, SqlOptions options, Map<String, Savepoint> savepoints) {
    super(helper, options);
    this.op = op;
    this.savepoints = savepoints;
  }

  @Override
  public R execute(Connection conn) throws SQLException {
    switch (op.kind()) {
      case CREATE:
        Savepoint sp = conn.setSavepoint(op.name());
        savepoints.put(op.name(), sp);
        break;
      case ROLLBACK_TO:
        Savepoint rollbackSp = savepoints.get(op.name());
        if (rollbackSp == null) {
          throw new SQLException("Unknown savepoint: " + op.name());
        }
        conn.rollback(rollbackSp);
        break;
      case RELEASE:
        Savepoint releaseSp = savepoints.remove(op.name());
        if (releaseSp == null) {
          throw new SQLException("Unknown savepoint: " + op.name());
        }
        conn.releaseSavepoint(releaseSp);
        break;
    }
    return op.result();
  }
}
