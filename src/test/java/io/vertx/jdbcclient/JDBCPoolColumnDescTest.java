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

package io.vertx.jdbcclient;

import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JDBCPoolColumnDescTest extends ClientTestBase {

  private static final JDBCConnectOptions options = new JDBCConnectOptions()
    .setJdbcUrl("jdbc:hsqldb:mem:" + JDBCPoolColumnDescTest.class.getSimpleName() + "?shutdown=true");

  private static final List<String> SQL = new ArrayList<>();

  static {
    SQL.add("DROP TABLE IF EXISTS t");
    SQL.add("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20), scores INT ARRAY)");
    SQL.add("INSERT INTO t VALUES 1, 'John', ARRAY[157,215,78]");
  }

  public static void resetDb() throws Exception {
    Connection conn = DriverManager.getConnection(options.getJdbcUrl());
    for (String sql : SQL) {
      conn.createStatement().execute(sql);
    }
  }

  @Before
  public void setUp() throws Exception {
    resetDb();
    super.setUp();
  }

  @Override
  protected JDBCConnectOptions connectOptions() {
    return options;
  }

  @Test
  public void testColumnDesc(TestContext should) {
    client.query("SELECT id AS key, name, scores FROM t").execute(should.asyncAssertSuccess(rows -> {
      should.verify(v -> {
        assertEquals(1, rows.size());
        List<ColumnDescriptor> columnDescriptors = rows.columnDescriptors();
        assertEquals(3, columnDescriptors.size());
        verifyDesc(columnDescriptors.get(0), "KEY", false, "INTEGER", JDBCType.INTEGER);
        verifyDesc(columnDescriptors.get(1), "NAME", false, "VARCHAR", JDBCType.VARCHAR);
        verifyDesc(columnDescriptors.get(2), "SCORES", true, "INTEGER ARRAY", JDBCType.ARRAY);
      });
    }));
  }

  private static void verifyDesc(ColumnDescriptor desc, String name, boolean array, String typeName, JDBCType jdbcType) {
    assertEquals(name, desc.name());
    assertEquals(array, desc.isArray());
    assertEquals(typeName, desc.typeName());
    assertEquals(jdbcType, desc.jdbcType());
  }
}
