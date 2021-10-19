/*
 * Copyright (c) 2011-2021 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.jdbcclient.impl.actions;

import io.vertx.ext.jdbc.impl.actions.JDBCTypeProvider;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JDBCColumnDescriptor implements ColumnDescriptor {

  private final String name;
  private final String typeName;
  private final JDBCType jdbcType;

  public JDBCColumnDescriptor(ResultSetMetaData metaData, JDBCTypeProvider provider, int i) throws SQLException {
    name = metaData.getColumnLabel(i);
    typeName = metaData.getColumnTypeName(i);
    jdbcType = provider.apply(i);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean isArray() {
    return jdbcType == JDBCType.ARRAY;
  }

  @Override
  public String typeName() {
    return typeName;
  }

  @Override
  public JDBCType jdbcType() {
    return jdbcType;
  }
}
