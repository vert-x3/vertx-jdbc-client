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
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.test.core.VertxTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="mailto:pmlopes@gmail.com">Paulo Lopes</a>
 */
public class JDBCQueryTimeoutTest extends VertxTestBase {

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
            .put("url", "jdbc:derby:memory:myDB2;create=true")
            .put("driver_class", "org.apache.derby.jdbc.EmbeddedDriver");
  }

  @Test
  public void testQueryTimeout() {
    String sql = "{call NAP(1) + NAP(1) + NAP(1) + NAP(1) + NAP(1)}";

    final SQLConnection conn = connection();

    conn.execute("CREATE FUNCTION NAP() returns INT PARAMETER STYLE JAVA reads sql data language JAVA EXTERNAL NAME 'io.vertx.ext.jdbc.Functions.nap'", onSuccess(res -> {
      conn.setQueryTimeout(1).call(sql, onFailure(resultSet -> {
        assertNotNull(resultSet);
//        assertEquals(1, resultSet.getResults().size());
//        // we expect a String since UUID will be converted with the fallback mode
//        assertNotNull(resultSet.getResults().get(0).getString(0));
        testComplete();
      }));

    }));


    await();
  }

  @Test
  public void testMultiSelect() {
    String sql = "{ call MS() }";

    final SQLConnection conn = connection();

    conn.execute("CREATE PROCEDURE MS() PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 2 EXTERNAL NAME 'io.vertx.ext.jdbc.Functions.multiSelect'", onSuccess(res -> {
      conn.call(sql, onSuccess(resultSet -> {
        assertNotNull(resultSet);
        assertNotNull(resultSet.getNext());
        testComplete();
      }));

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
