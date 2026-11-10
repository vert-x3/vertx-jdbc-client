/*
 * Copyright (c) 2011-2026 The original author or authors
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

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link JDBCConnectOptions#setUseVirtualThreads(boolean)} option.
 * <p>
 * The tests use H2 and Java aliases to observe the thread executing the blocking JDBC calls.
 */
@RunWith(VertxUnitRunner.class)
public class JDBCPoolVirtualThreadsTest {

  private static final String ALIAS_CLASS = "io.vertx.jdbcclient.JDBCPoolVirtualThreadsTest";

  /**
   * Threads which established a physical connection, recorded by the H2 {@code INIT=} clause.
   * The clause runs again for every physical connection, so the number of entries is not predictable.
   */
  private static final Queue<Thread> CONNECT_THREADS = new ConcurrentLinkedQueue<>();

  /**
   * Threads which executed a query, recorded by {@code SELECT RECORD_THREAD()}.
   */
  private static final Queue<Thread> QUERY_THREADS = new ConcurrentLinkedQueue<>();

  private static final List<Integer> ORDER = new CopyOnWriteArrayList<>();

  /**
   * Invoked by H2 on the thread establishing a physical connection.
   */
  public static int recordConnectThread() {
    CONNECT_THREADS.add(Thread.currentThread());
    return 1;
  }

  /**
   * Invoked by H2 on the thread executing the query.
   */
  public static int recordThread() {
    QUERY_THREADS.add(Thread.currentThread());
    return 1;
  }

  /**
   * Invoked by H2 on the thread executing the JDBC call.
   */
  public static int recordOrder(int value) {
    ORDER.add(value);
    return value;
  }

  private Vertx vertx;
  private Pool pool;

  @Before
  public void setUp() {
    CONNECT_THREADS.clear();
    QUERY_THREADS.clear();
    ORDER.clear();
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown(TestContext should) {
    // Do not compose the two close operations: the composition would be bound to the pool context, and the
    // completion handler of such a composition is never invoked because the event loop is shut down by then.
    if (pool == null) {
      vertx.close().onComplete(should.asyncAssertSuccess());
    } else {
      pool.close().onComplete(should.asyncAssertSuccess(v -> vertx.close().onComplete(should.asyncAssertSuccess())));
    }
  }

  private boolean virtualThreadsAvailable() {
    return ((VertxInternal) vertx).isVirtualThreadAvailable();
  }

  /**
   * The project is compiled for Java 11, hence {@code Thread#isVirtual} (Java 21+) is invoked reflectively.
   */
  private static boolean isVirtual(Thread thread) {
    try {
      return (Boolean) Thread.class.getMethod("isVirtual").invoke(thread);
    } catch (NoSuchMethodException e) {
      return false;
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private JDBCConnectOptions options(boolean useVirtualThreads) {
    // INIT runs on the thread establishing the connection, and runs again for every physical connection
    String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";INIT="
      + "CREATE ALIAS IF NOT EXISTS RECORD_CONNECT_THREAD FOR \"" + ALIAS_CLASS + ".recordConnectThread\"\\;"
      + "CREATE ALIAS IF NOT EXISTS RECORD_THREAD FOR \"" + ALIAS_CLASS + ".recordThread\"\\;"
      + "CREATE ALIAS IF NOT EXISTS RECORD_ORDER FOR \"" + ALIAS_CLASS + ".recordOrder\"\\;"
      + "CALL RECORD_CONNECT_THREAD()";
    return new JDBCConnectOptions()
      .setJdbcUrl(url)
      .setUseVirtualThreads(useVirtualThreads);
  }

  private Pool createPool(boolean useVirtualThreads) {
    pool = JDBCPool.pool(vertx, options(useVirtualThreads), new PoolOptions().setMaxSize(1));
    return pool;
  }

  /**
   * Asserts the virtualness of every thread recorded for a given call site.
   * The number of entries is not asserted, only that the call site was observed at least once.
   */
  private static void assertThreads(TestContext should, String callSite, Queue<Thread> recorded, boolean expectVirtual) {
    should.assertFalse(recorded.isEmpty(), "No thread recorded for call site '" + callSite + "'");
    for (Thread thread : recorded) {
      if (expectVirtual) {
        should.assertTrue(isVirtual(thread), "Call site '" + callSite + "': expected a virtual thread but was " + thread);
      } else {
        should.assertFalse(isVirtual(thread), "Call site '" + callSite + "': unexpected virtual thread " + thread);
      }
    }
  }

  @Test
  public void testOptionDefaultValue() {
    JDBCConnectOptions options = new JDBCConnectOptions();
    assertFalse(options.getUseVirtualThreads());
  }

  @Test
  public void testOptionCopy() {
    JDBCConnectOptions options = new JDBCConnectOptions().setUseVirtualThreads(true);
    assertTrue(new JDBCConnectOptions(options).getUseVirtualThreads());
  }

  @Test
  public void testOptionJsonRoundTrip() {
    JDBCConnectOptions options = new JDBCConnectOptions().setUseVirtualThreads(true);
    JsonObject json = options.toJson();
    assertTrue(json.getBoolean("useVirtualThreads"));
    assertTrue(new JDBCConnectOptions(json).getUseVirtualThreads());
    assertFalse(new JDBCConnectOptions(new JDBCConnectOptions().toJson()).getUseVirtualThreads());
  }

  @Test
  public void testPlatformThreadsWhenOptionIsOff(TestContext should) {
    createPool(false)
      .query("SELECT RECORD_THREAD()")
      .execute()
      .onComplete(should.asyncAssertSuccess(rows -> {
        assertThreads(should, "connect", CONNECT_THREADS, false);
        assertThreads(should, "query", QUERY_THREADS, false);
      }));
  }

  @Test
  public void testVirtualThreadsWhenOptionIsOn(TestContext should) {
    Assume.assumeTrue("Virtual threads are not available", virtualThreadsAvailable());
    createPool(true)
      .query("SELECT RECORD_THREAD()")
      .execute()
      .onComplete(should.asyncAssertSuccess(rows -> {
        assertThreads(should, "connect", CONNECT_THREADS, true);
        assertThreads(should, "query", QUERY_THREADS, true);
      }));
  }

  /**
   * Asserts the contract "with the option on, blocking calls run on virtual threads if and only if virtual threads
   * are available". In particular, when virtual threads are not available, the pool must still work on platform threads.
   * <p>
   * On a JVM without virtual threads (the build also runs on Java 11), this test genuinely exercises the fallback
   * branch of the connection factory. On Java 21+, virtual thread availability is a constant of the Vert.x core
   * internals ({@code VertxInternal#isVirtualThreadAvailable}), so the fallback branch cannot be reached in-process
   * without reflection or mocking internals, and this test then asserts the virtual thread branch instead. Which side
   * of the contract gets exercised therefore depends on the runtime JVM.
   */
  @Test
  public void testOptionOnUsesVirtualThreadsIffAvailable(TestContext should) {
    boolean expectVirtual = virtualThreadsAvailable();
    createPool(true)
      .query("SELECT RECORD_THREAD()")
      .execute()
      .onComplete(should.asyncAssertSuccess(rows -> {
        should.assertEquals(1, rows.size());
        assertThreads(should, "connect", CONNECT_THREADS, expectVirtual);
        assertThreads(should, "query", QUERY_THREADS, expectVirtual);
      }));
  }

  @Test
  public void testQueryWorksWhenOptionIsOn(TestContext should) {
    // Virtual threads when available, platform threads otherwise (fallback)
    createPool(true)
      .query("SELECT 1")
      .execute()
      .onComplete(should.asyncAssertSuccess(rows -> {
        should.assertEquals(1, rows.size());
        should.assertEquals(1, rows.iterator().next().getInteger(0));
      }));
  }

  @Test
  public void testTransactionWhenOptionIsOn(TestContext should) {
    createPool(true)
      .withTransaction(conn -> conn
        .query("CREATE TABLE vt_tx (id INT PRIMARY KEY)").execute()
        .compose(v -> conn.query("INSERT INTO vt_tx VALUES (1)").execute())
        .compose(v -> conn.query("SELECT id FROM vt_tx").execute()))
      .onComplete(should.asyncAssertSuccess(rows -> {
        should.assertEquals(1, rows.size());
      }));
  }

  @Test
  public void testOrderIsPreservedWhenOptionIsOn(TestContext should) {
    int num = 20;
    createPool(true).getConnection().onComplete(should.asyncAssertSuccess(conn -> {
      List<Future<?>> futures = new ArrayList<>();
      // Do not wait for completion of a query before submitting the next one
      for (int i = 0; i < num; i++) {
        futures.add(conn.query("SELECT RECORD_ORDER(" + i + ")").execute());
      }
      Future.all(futures)
        .eventually(() -> conn.close())
        .onComplete(should.asyncAssertSuccess(v -> {
          should.assertEquals(num, ORDER.size());
          for (int i = 0; i < num; i++) {
            should.assertEquals(i, ORDER.get(i));
          }
        }));
    }));
  }
}
