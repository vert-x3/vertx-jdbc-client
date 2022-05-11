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
public class JDBCCustomTXIsolationTest extends JDBCClientTestBase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    client = JDBCClient.create(vertx, DBConfigs.h2(JDBCCustomTXIsolationTest.class));
  }

  @Test
  public void testGetSet() {
    SQLConnection conn = connection();

    conn.getTransactionIsolation(txIsolation -> {
      if (txIsolation.failed()) {
        fail(txIsolation.cause());
        return;
      }

      conn.setTransactionIsolation(txIsolation.result(), res -> {
        if (res.failed()) {
          fail(res.cause());
        }

        testComplete();
      });
    });

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
