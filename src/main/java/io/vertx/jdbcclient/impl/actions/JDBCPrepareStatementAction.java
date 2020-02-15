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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCPrepareStatementAction extends AbstractJDBCAction<io.vertx.sqlclient.impl.PreparedStatement> {

  private final String sql;

  public JDBCPrepareStatementAction(JDBCStatementHelper helper, SQLOptions options, String sql) {
    super(helper, options);
    this.sql = sql;
  }

  @Override
  public io.vertx.sqlclient.impl.PreparedStatement execute(Connection conn) throws SQLException {
    java.sql.PreparedStatement ps = conn.prepareStatement(sql);
    return new JDBCPreparedStatement(sql, ps);
  }
}
