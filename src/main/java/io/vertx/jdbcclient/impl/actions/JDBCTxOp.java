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

import io.vertx.ext.jdbc.impl.actions.AbstractJDBCAction;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.sqlclient.impl.command.TxCommand;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class JDBCTxOp extends AbstractJDBCAction<Void> {

  private final TxCommand op;

  public JDBCTxOp(JDBCStatementHelper helper, TxCommand op, SQLOptions options) {
    super(helper, options);
    this.op = op;
  }

  @Override
  public Void execute(Connection conn) throws SQLException {
    if (op == TxCommand.BEGIN) {
      conn.setAutoCommit(false);
    } else {
      try {
        if (op == TxCommand.COMMIT) {
          conn.commit();
        } else {
          conn.rollback();
        }
      } finally {
        conn.setAutoCommit(false);
      }
    }
    return null;
  }
}
