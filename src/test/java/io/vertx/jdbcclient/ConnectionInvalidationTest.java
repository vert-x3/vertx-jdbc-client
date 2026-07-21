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

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A JDBC connection cannot notify the pool of its own death the way the socket based clients do, so
 * the driver must report fatal failures to the pool instead. Before this was implemented, a pooled
 * connection that died while pooled (database restart, server side idle timeout, a pooling
 * {@code DataSource} invalidating the physical connection) was recycled forever: every lease of it
 * failed with the same connection error until the process was restarted.
 */
@RunWith(VertxUnitRunner.class)
public class ConnectionInvalidationTest {

  /**
   * Hands out real hsqldb connections wrapped in a proxy with a kill switch: once killed, every
   * call except {@code close()}/{@code isClosed()} throws a fatal connection error, mimicking a
   * connection the server has dropped.
   */
  static class KillableDataSource implements DataSource {

    final List<AtomicBoolean> killSwitches = new CopyOnWriteArrayList<>();

    @Override
    public Connection getConnection() throws SQLException {
      Connection real = DriverManager.getConnection("jdbc:hsqldb:mem:" + ConnectionInvalidationTest.class.getSimpleName());
      AtomicBoolean killed = new AtomicBoolean();
      killSwitches.add(killed);
      InvocationHandler handler = (proxy, method, args) -> {
        String name = method.getName();
        if (killed.get() && !name.equals("close") && !name.equals("isClosed") && !name.equals("isValid")) {
          throw new SQLNonTransientConnectionException("connection failure", "08003");
        }
        try {
          return method.invoke(real, args);
        } catch (InvocationTargetException e) {
          throw e.getCause();
        }
      };
      return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, handler);
    }

    void killAll() {
      for (AtomicBoolean killed : killSwitches) {
        killed.set(true);
      }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
    }

    @Override
    public void setLoginTimeout(int seconds) {
    }

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  private Vertx vertx;
  private KillableDataSource dataSource;
  private Pool client;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    dataSource = new KillableDataSource();
    client = JDBCPool.pool(vertx, dataSource, new PoolOptions().setMaxSize(1));
  }

  @After
  public void after(TestContext ctx) {
    client.close().onComplete(ctx.asyncAssertSuccess(v -> {
      vertx.close().onComplete(ctx.asyncAssertSuccess());
    }));
  }

  @Test
  public void testPoolRecoversWhenPooledConnectionDies(TestContext should) {
    Async test = should.async();
    // lease the single pooled connection once so it is established and recycled
    client
      .query("SELECT 1 FROM (VALUES(0))")
      .execute()
      .onFailure(should::fail)
      .onSuccess(rows1 -> {
        // the connection dies while sitting in the pool
        dataSource.killAll();
        // this lease draws the dead connection and must fail
        client
          .query("SELECT 1 FROM (VALUES(0))")
          .execute()
          .onSuccess(rows -> should.fail("query on a dead connection should not succeed"))
          .onFailure(err -> {
            // the dead connection was reported and must be removed: the pool serves a fresh one
            // (removal is asynchronous, so allow a few attempts)
            awaitRecovery(should, test, 10);
          });
      });
  }

  private void awaitRecovery(TestContext should, Async test, int remainingAttempts) {
    client
      .query("SELECT 1 FROM (VALUES(0))")
      .execute()
      .onSuccess(rows -> {
        should.assertEquals(1, rows.size());
        test.complete();
      })
      .onFailure(err -> {
        if (remainingAttempts > 0) {
          vertx.setTimer(100, id -> awaitRecovery(should, test, remainingAttempts - 1));
        } else {
          should.fail("the pool did not recover after the pooled connection died: " + err);
        }
      });
  }
}
