/*
 * Copyright (c) 2011-2022 Contributors to the Eclipse Foundation
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
import io.vertx.jdbcclient.data.MyObject;
import io.vertx.jdbcclient.data.MyObjectRowMapper;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlClientTemplatesTest extends ClientTestBase {

  private static final JDBCConnectOptions options = new JDBCConnectOptions()
    .setJdbcUrl("jdbc:h2:mem:" + SqlClientTemplatesTest.class.getSimpleName() + ";DB_CLOSE_DELAY=-1");

  private static final List<String> SQL = new ArrayList<>();

  static {
    SQL.add("drop table if exists my_objects");
    SQL.add("create table my_objects (id int, status varchar(255) );");
    SQL.add("insert into my_objects values (1, 'ABC');");
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
  public void testCanReadEnum(TestContext should) {
    SqlTemplate
      .forQuery(client, "SELECT id, status FROM my_objects WHERE id = #{id}")
      .mapTo(MyObjectRowMapper.INSTANCE)
      .execute(Collections.singletonMap("id", 1), should.asyncAssertSuccess(rows -> {
        should.assertEquals(1, rows.size());
        should.assertEquals(MyObject.Status.ABC, rows.iterator().next().getType());
      }));
  }
}
