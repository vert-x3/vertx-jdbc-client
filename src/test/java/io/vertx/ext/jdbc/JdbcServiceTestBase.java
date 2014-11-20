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

package io.vertx.ext.jdbc;

import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public abstract class JdbcServiceTestBase extends VertxTestBase {
  protected JdbcService service;

  private static final List<String> CREATE_SQL = new ArrayList<>();
  private static final List<String> DESTORY_SQL = new ArrayList<>();

  static {
    CREATE_SQL.add("create table if not exists foo (id int, lname varchar(255), fname varchar(255) );");
    CREATE_SQL.add("insert into foo values (1, 'doe', 'john');");
    DESTORY_SQL.add("drop table foo;");
  }

  @BeforeClass
  public static void createDb() throws Exception {
    Connection conn = DriverManager.getConnection(config().getString("url"));
    for (String sql : CREATE_SQL) {
      conn.createStatement().execute(sql);
    }
  }

  @AfterClass
  public static void destroyDb() throws Exception {
    Connection conn = DriverManager.getConnection(config().getString("url"));
    for (String sql : DESTORY_SQL) {
      conn.createStatement().execute(sql);
    }
  }

  protected static JsonObject config() {
    return new JsonObject()
      .put("driver", "org.hsqldb.jdbcDriver")
      .put("url", "jdbc:hsqldb:mem:test?shutdown=true");
  }

  @Test
  public void testSelect() {
    service.select("foo", onSuccess(results -> {
      assertNotNull(results);
      assertEquals(1, results.size());
      JsonObject result = results.get(0);
      assertEquals(1, (int) result.getInteger("ID"));
      assertEquals("doe", result.getString("LNAME"));
      assertEquals("john", result.getString("FNAME"));
      testComplete();
    }));

    await();
  }
}
