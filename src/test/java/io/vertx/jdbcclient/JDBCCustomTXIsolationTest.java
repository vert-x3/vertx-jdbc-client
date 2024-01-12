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

package io.vertx.jdbcclient;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.PoolOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author <a href="mailto:pmlopes@gmail.com">Paulo Lopes</a>
 */
@RunWith(VertxUnitRunner.class)
public class JDBCCustomTXIsolationTest extends ClientTestBase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    client = JDBCPool.pool(vertx, DataSourceConfigs.h2(JDBCCustomTXIsolationTest.class), new PoolOptions());
  }

  @Test
  public void testGetSet(TestContext ctx) {
    client.getConnection()
      .compose(conn -> JDBCUtils
        .getTransactionIsolation(conn)
        .compose(level -> JDBCUtils
          .setTransactionIsolation(conn, level))).onComplete(ctx.asyncAssertSuccess());
  }
}
