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

package io.vertx.ext.jdbc.impl;

import io.vertx.core.*;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Shareable;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.sql.SQLConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCClientImpl implements JDBCClient {

  private static final String DS_LOCAL_MAP_NAME = "__vertx.JDBCClient.datasources";

  private final Vertx vertx;
  private final DataSourceHolder holder;

  // We use this executor to execute getConnection requests
  private final ExecutorService exec;
  private final DataSource ds;
  private final PoolMetrics metrics;

  /*
  Create client with specific datasource
   */
  public JDBCClientImpl(Vertx vertx, DataSource dataSource) {
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(dataSource);
    this.vertx = vertx;
    this.holder = new DataSourceHolder((VertxInternal) vertx, dataSource);
    this.exec = holder.exec();
    this.ds = dataSource;
    this.metrics = holder.metrics;
  }

  /*
  Create client with shared datasource
   */
  public JDBCClientImpl(Vertx vertx, JsonObject config, String datasourceName) {
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(config);
    Objects.requireNonNull(datasourceName);
    this.vertx = vertx;
    this.holder = lookupHolder(datasourceName, config);
    this.exec = holder.exec();
    this.ds = holder.ds();
    this.metrics = holder.metrics;
  }

  @Override
  public void close() {
    holder.close();
  }

  @Override
  public JDBCClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
    Context ctx = vertx.getOrCreateContext();
    boolean enabled = metrics != null && metrics.isEnabled();
    Object metric = enabled ? metrics.taskSubmitted() : null;
    PoolMetrics metrics = enabled ? this.metrics : null;
    exec.execute(() -> {
      Future<SQLConnection> res = Future.future();
      try {
        /*
        This can block until a connection is free.
        We don't want to do that while running on a worker as we can enter a deadlock situation as the worker
        might have obtained a connection, and won't release it until it is run again
        There is a general principle here:
        *User code* should be executed on a worker and can potentially block, it's up to the *user* to deal with
        deadlocks that might occur there.
        If the *service code* internally blocks waiting for a resource that might be obtained by *user code*, then
        this can cause deadlock, so the service should ensure it never does this, by executing such code
        (e.g. getConnection) on a different thread to the worker pool.
        We don't want to use the vert.x internal pool for this as the threads might end up all blocked preventing
        other important operations from occurring (e.g. async file access)
        */
        Connection conn = ds.getConnection();
        if (metrics != null) {
          metrics.taskBegin(metric);
        }
        SQLConnection sconn = new JDBCConnectionImpl(vertx, conn, metrics, metric);
        res.complete(sconn);
      } catch (SQLException e) {
        res.fail(e);
      }
      ctx.runOnContext(v -> res.setHandler(handler));
    });
    return this;
  }

  private DataSourceHolder lookupHolder(String datasourceName, JsonObject config) {
    synchronized (vertx) {
      LocalMap<String, DataSourceHolder> map = vertx.sharedData().getLocalMap(DS_LOCAL_MAP_NAME);
      DataSourceHolder theHolder = map.get(datasourceName);
      if (theHolder == null) {
        theHolder = new DataSourceHolder((VertxInternal) vertx, config, () -> removeFromMap(map, datasourceName), datasourceName);
        map.put(datasourceName, theHolder);
      } else {
        theHolder.incRefCount();
      }
      return theHolder;
    }
  }

  private void removeFromMap(LocalMap<String, DataSourceHolder> map, String dataSourceName) {
    synchronized (vertx) {
      map.remove(dataSourceName);
      if (map.isEmpty()) {
        map.close();
      }
    }
  }

  private class DataSourceHolder implements Shareable {

    private final VertxInternal vertx;
    DataSourceProvider provider;
    JsonObject config;
    Runnable closeRunner;
    DataSource ds;
    PoolMetrics metrics;
    ExecutorService exec;
    int refCount = 1;
    String name;

    public DataSourceHolder(VertxInternal vertx, DataSource ds) {
      this.ds = ds;
      this.metrics = vertx.metricsSPI().createMetrics(ds, UUID.randomUUID().toString(), -1);
      this.vertx = vertx;
    }

    public DataSourceHolder(VertxInternal vertx, JsonObject config, Runnable closeRunner, String name) {
      this.config = config;
      this.closeRunner = closeRunner;
      this.vertx = vertx;
      this.name = name;
    }

    synchronized DataSource ds() {
      if (ds == null) {
        String providerClass = config.getString("provider_class");
        if (providerClass == null) {
          providerClass = DEFAULT_PROVIDER_CLASS;
        }

        if (Thread.currentThread().getContextClassLoader() != null) {
          try {
            // Try with the TCCL
            Class clazz = Thread.currentThread().getContextClassLoader().loadClass(providerClass);
            provider = (DataSourceProvider) clazz.newInstance();
            ds = provider.getDataSource(config);
            int poolSize = provider.maximumPoolSize(ds, config);
            metrics = vertx.metricsSPI().createMetrics(ds, name, poolSize);
            return ds;
          } catch (ClassNotFoundException e) {
            // Next try.
          } catch (InstantiationException | SQLException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }

        try {
          // Try with the classloader of the current class.
          Class clazz = this.getClass().getClassLoader().loadClass(providerClass);
          provider = (DataSourceProvider) clazz.newInstance();
          ds = provider.getDataSource(config);
          int poolSize = provider.maximumPoolSize(ds, config);
          metrics = vertx.metricsSPI().createMetrics(ds, name, poolSize);
          return ds;
        } catch (ClassNotFoundException | InstantiationException | SQLException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }

      return ds;
    }

    synchronized ExecutorService exec() {
      if (exec == null) {
        exec = new ThreadPoolExecutor(1, 1,
            1000L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            (r -> new Thread(r, "vertx-jdbc-service-get-connection-thread")));
      }
      return exec;
    }

    synchronized void incRefCount() {
      refCount++;
    }

    synchronized void close() {
      if (--refCount == 0) {
        if (metrics != null) {
          metrics.close();
        }
        if (provider != null) {
          vertx.executeBlocking(future -> {
            try {
              provider.close(ds);
              future.complete();
            } catch (SQLException e) {
              future.fail(e);
            }
          }, null);
        }
        if (exec != null) {
          exec.shutdown();
        }
        if (closeRunner != null) {
          closeRunner.run();
        }
      }
    }
  }
}
