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

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Tuple;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class OracleTest {

  @Rule
  public final RunTestOnContext rule = new RunTestOnContext();

  @Test
  @Ignore("Cannot run this in CI as we can't install Oracle")
  public void testSimple(TestContext should) {
    final Async test = should.async();

    final Pool pool = JDBCPool.pool(
      rule.vertx(),
      new JDBCConnectOptions()
        .setJdbcUrl("jdbc:oracle:thin:@127.0.0.1:1521:xe")
        .setUser("sys as sysdba")
        .setPassword("vertx"),
      new PoolOptions());

    pool.preparedQuery("DELETE FROM product WHERE id = ?")
      .execute(Tuple.of("missing-id"))
      .onFailure(should::fail)
      .onSuccess(rowSet -> {
        test.complete();
      });
  }

  @Test
  @Ignore("Cannot run this in CI as we can't install Oracle")
  public void testBlocking(TestContext should) {
//    CREATE OR REPLACE FUNCTION MYSCHEMA.TEST_SLEEP
//      (
//        TIME_  IN  NUMBER
//      )
//    RETURN INTEGER IS
//      BEGIN
//    DBMS_LOCK.sleep(seconds => TIME_);
//    RETURN 1;
//    EXCEPTION
//    WHEN OTHERS THEN
//      RAISE;
//    RETURN 1;
//    END TEST_SLEEP;

    final Async test = should.async();

    final Pool pool = JDBCPool.pool(
      rule.vertx(),
      new JDBCConnectOptions()
        .setJdbcUrl("jdbc:oracle:thin:@127.0.0.1:1521:xe")
        .setUser("sys as sysdba")
        .setPassword("vertx"),
      new PoolOptions());

    pool.preparedQuery("SELECT TEST_SLEEP(10.5) FROM DUAL")
      .execute()
      .onFailure(should::fail)
      .onSuccess(rowSet -> {
        test.complete();
      });
  }

  @Test
  @Ignore
  public void slowDecode(TestContext should) {
//    CREATE TABLE "CONFIG" (
//      "ID"     NUMBER(10, 0),
//      "KEY1"   CLOB DEFAULT NULL,
//      "KEY2"   CLOB DEFAULT NULL,
//      "KEY3"   CLOB DEFAULT NULL,
//      "KEY4"   CLOB DEFAULT NULL,
//      "KEY5"   CLOB DEFAULT NULL,
//      "KEY6"   CLOB DEFAULT NULL
//)

    final Async test = should.async();

    final Pool pool = JDBCPool.pool(
      rule.vertx(),
      new JDBCConnectOptions()
        .setJdbcUrl("jdbc:oracle:thin:@127.0.0.1:1521:xe")
        .setUser("sys as sysdba")
        .setPassword("vertx"),
      new PoolOptions());

    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < 30000; i++) {
      sb.append('a');
    }

    final Runnable queryIt = () -> {
      final long start = System.currentTimeMillis();
      pool.preparedQuery("select * FROM CONFIG")
        .execute()
        .onFailure(should::fail)
        .onSuccess(rowSet -> {
          long end = System.currentTimeMillis();
          System.out.println(end - start);
          test.complete();
        });
    };

    queryIt.run();

//    final AtomicInteger cnt = new AtomicInteger();
//    PreparedQuery<?> query = pool.preparedQuery("insert into CONFIG (ID, KEY3) VALUES (? , ?)");
//    final Runnable insertIt = new Runnable() {
//      @Override
//      public void run() {
//        query
//          .execute(Tuple.of(cnt.get(), sb.toString()))
//          .onFailure(should::fail)
//          .onSuccess(rowSet -> {
//            if (cnt.incrementAndGet() == 2000) {
//              queryIt.run();
//            } else {
//              run();
//            }
//          });
//      }
//    };
//
//    insertIt.run();
  }
}
