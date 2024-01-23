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

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Ignore("We can't run Oracle on CI")
@RunWith(VertxUnitRunner.class)
public class OracleRowIdTest {

  // docker run --rm -p 1521:1521 -e ORACLE_PASSWORD=vertx gvenzl/oracle-xe

  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  private static final List<String> SQL = new ArrayList<>();

  private Pool client;

  static {
    SQL.add("DROP TABLE vegetables");
    SQL.add("CREATE TABLE vegetables (" +
      "  id        NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1)," +
      "  name      VARCHAR2(40) NOT NULL," +
      "  amount    INT," +
      "  CONSTRAINT vegetables_pk PRIMARY KEY (id))");
  }

  @Before
  public void setUp() throws Exception {
    String jdbcUrl = "jdbc:oracle:thin:@127.0.0.1:1521:xe";
    String username = "system";
    String password = "vertx";
    Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
    for (String sql : SQL) {
      try {
        conn.createStatement().execute(sql);
      } catch (SQLException ignore) {
      }
    }

    client = JDBCPool.pool(rule.vertx(), new JDBCConnectOptions()
      .setJdbcUrl(jdbcUrl)
      .setUser(username)
      .setPassword(password), new PoolOptions().setMaxSize(1));
  }

  @Test
  public void rowIdTest(TestContext should) {
    final Async test = should.async();

    client
      .preparedQuery("INSERT INTO vegetables (name, amount) VALUES (?, ?)")
      .execute(Tuple.of("pickle", 5))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertNotNull(rows);
        Row lastInsertId = rows.property(JDBCPool.GENERATED_KEYS);
        byte[] newId = lastInsertId.get(byte[].class, 0);
        should.assertNotNull(newId);
        client
          .preparedQuery("SELECT * FROM vegetables WHERE rowid = ?")
          .execute(Tuple.of(new String(newId)))
          .onFailure(should::fail)
          .onSuccess(rows1 -> {
            for (Row row : rows1) {
              // work...
            }
            test.complete();
          });
      });
  }
}
