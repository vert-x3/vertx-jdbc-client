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

import io.vertx.ext.jdbc.spi.JDBCColumnDescriptorProvider;
import io.vertx.sqlclient.impl.ParamDesc;
import io.vertx.sqlclient.impl.PreparedStatement;
import io.vertx.sqlclient.impl.RowDesc;
import io.vertx.sqlclient.impl.TupleInternal;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JDBCPreparedStatement implements PreparedStatement {

  private final String sql;
  private final java.sql.PreparedStatement preparedStatement;
  private final RowDesc rowDesc;
  private final ParamDesc paramDesc;

  public JDBCPreparedStatement(String sql, java.sql.PreparedStatement preparedStatement) throws SQLException {

    ResultSetMetaData metaData = preparedStatement.getMetaData();
    JDBCRowDesc rowDesc;
    if (metaData != null) {
      // Not a SELECT
      int cols = metaData.getColumnCount();
      JDBCColumnDescriptorProvider provider = JDBCColumnDescriptorProvider.fromResultMetaData(metaData);
      rowDesc = new JDBCRowDesc(provider, cols);
    } else {
      rowDesc = new JDBCRowDesc();
    }

    this.sql = sql;
    this.rowDesc = rowDesc;
    this.paramDesc = new ParamDesc();
    this.preparedStatement = preparedStatement;
  }

  public java.sql.PreparedStatement preparedStatement() {
    return preparedStatement;
  }

  @Override
  public ParamDesc paramDesc() {
    return paramDesc;
  }

  @Override
  public RowDesc rowDesc() {
    return rowDesc;
  }

  @Override
  public String sql() {
    return sql;
  }

  @Override
  public String prepare(TupleInternal values) {
    return null;
  }
}
