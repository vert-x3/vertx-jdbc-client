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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.test.core.VertxTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="mailto:pmlopes@gmail.com">Paulo Lopes</a>
 */
public class JDBCStoredProcedure2Test extends VertxTestBase {

  protected SQLClient client;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    client = JDBCClient.createNonShared(vertx, config());
  }

  @After
  public void after() throws Exception {
    client.close();
    super.after();
  }

  protected static JsonObject config() {
    return new JsonObject()
        .put("url", "jdbc:mysql://localhost/test")
        .put("driver_class", "com.mysql.jdbc.Driver")
        .put("user", "root")
        .put("password", "mypassword");
  }


  /**
   * This test has been marked as ignored since it can only run on mysql and it might not be 100% correct.
   * A procedure is not supposed to return data by definition however MySQL allows this mix...
   *
   * This requires the following proc to be installed on a MySQL server:
   *
   * create DATABASE test;
   * use test;
   *
   * DROP PROCEDURE `proc_test`;
   *
   * DELIMITER $$
   * CREATE PROCEDURE `proc_test`(IN firstname varchar(45), OUT lastname varchar(45))
   * BEGIN
   *   select concat(firstname, '!!!') into lastname;
   *   select now(6);
   * END$$
   * DELIMITER ;
   */
  @Test
  @Ignore
  public void testStoredProcedure1() {
    connection().callWithParams("{call proc_test(?, ?)}", new JsonArray().add("zepinos"), new JsonArray().addNull().add("VARCHAR"), onSuccess(resultSet -> {
      assertNotNull(resultSet);
      assertEquals(1, resultSet.getResults().size());
      assertEquals("zepinos!!!", resultSet.getOutput().getString(1));
      testComplete();
    }));

    await();
  }

  private SQLConnection connection() {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<SQLConnection> ref = new AtomicReference<>();
    client.getConnection(onSuccess(conn -> {
      ref.set(conn);
      latch.countDown();
    }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return ref.get();
  }
}
