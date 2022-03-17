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

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.jdbc.impl.actions.SQLValueProvider;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.JDBCDecoderImpl;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.jdbcclient.impl.AgroalCPDataSourceProvider;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import oracle.sql.TIMESTAMPTZ;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Ignore("We can't run Oracle on CI")
public class OracleRowIdTest extends ClientTestBase {

  private static final List<String> SQL = new ArrayList<>();

  private JDBCConnectOptions options;

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
    vertx = Vertx.vertx();
    DataSourceProvider provider = new AgroalCPDataSourceProvider(options, poolOptions());
    client = JDBCPool.pool(vertx, provider);
  }

  @Override
  protected JDBCConnectOptions connectOptions() {
    return options;
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
