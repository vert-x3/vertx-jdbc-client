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
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JDBCPoolColumnDescOracleExtTest extends ClientTestBase {

  private static final List<String> SQL = new ArrayList<>();

  private JDBCConnectOptions options;

  static {
    SQL.add("DROP TABLE my_table1");
    SQL.add("CREATE TABLE my_table1 (" +
      "    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY," +
      "    name VARCHAR2(100) NOT NULL," +
      "    created TIMESTAMP WITH TIME ZONE NOT NULL" +
      ")");
    SQL.add("INSERT INTO my_table1 (name, created) VALUES (" +
      "    'foo'," +
      "    TO_TIMESTAMP_TZ('1999-12-01 11:00:00 -8:00','YYYY-MM-DD HH:MI:SS TZH:TZM')" +
      ")");
  }

  @Before
  public void setUp() throws Exception {
    String jdbcUrl = "jdbc:oracle:thin:@127.0.0.1:1521:xe";
    String username = "sys as sysdba";
    String password = "vertx";
    Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
    for (String sql : SQL) {
      try {
        conn.createStatement().execute(sql);
      } catch (SQLException ignore) {
      }
    }
    options = new JDBCConnectOptions()
      .setJdbcUrl(jdbcUrl)
      .setUser(username)
      .setPassword(password);
    super.setUp();
  }

  @Override
  protected JDBCConnectOptions connectOptions() {
    return options;
  }

  @Test
  @Ignore("Cannot run this in CI as we can't install Oracle")
  public void testColumnDesc(TestContext should) {
    client.query("SELECT id, name, created FROM my_table1").execute(should.asyncAssertSuccess(rows -> {
      should.verify(v -> {
        assertEquals(1, rows.size());
        List<ColumnDescriptor> columnDescriptors = rows.columnDescriptors();
        assertEquals(3, columnDescriptors.size());
        verifyDesc(columnDescriptors.get(0), "ID", false, "NUMBER", JDBCType.NUMERIC);
        verifyDesc(columnDescriptors.get(1), "NAME", false, "VARCHAR2", JDBCType.VARCHAR);
        verifyDesc(columnDescriptors.get(2), "CREATED", false, "TIMESTAMP WITH TIME ZONE", JDBCType.OTHER);
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
