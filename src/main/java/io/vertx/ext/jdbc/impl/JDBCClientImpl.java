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
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.jdbc.impl.actions.AbstractJDBCAction;
import io.vertx.ext.jdbc.impl.actions.JDBCQuery;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.jdbc.impl.actions.JDBCUpdate;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
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

  private final VertxInternal vertx;
  private final String datasourceName;
  private final JsonObject config;
  private final Map<String, DataSourceHolder> holders;
  // Helper that can do param and result transforms, its behavior is defined by the
  // initial config and immutable after that moment. It is safe to reuse since there
  // is no state involved
  private final JDBCStatementHelper helper;

  private boolean closed;

  /**
   * Create client with specific datasource.
   */
  public JDBCClientImpl(Vertx vertx, DataSource dataSource) {
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(dataSource);
    this.vertx = (VertxInternal) vertx;
    datasourceName = UUID.randomUUID().toString();
    config = null;
    holders = vertx.sharedData().getLocalMap(DS_LOCAL_MAP_NAME);
    DataSourceHolder holder = new DataSourceHolder(dataSource, createExecutor(), createMetrics(datasourceName, -1));
    holders.put(datasourceName, holder);
    this.helper = new JDBCStatementHelper();
    setupCloseHook();
  }

  /**
   * Create client with shared datasource.
   */
  public JDBCClientImpl(Vertx vertx, JsonObject config, String datasourceName) {
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(config);
    Objects.requireNonNull(datasourceName);
    this.vertx = (VertxInternal) vertx;
    this.datasourceName = datasourceName;
    this.config = config;
    holders = vertx.sharedData().getLocalMap(DS_LOCAL_MAP_NAME);
    DataSourceProvider provider = createProvider();
    holders.compute(datasourceName, (k, h) -> h == null ? new DataSourceHolder(provider) : h.increment());
    this.helper = new JDBCStatementHelper(config);
    setupCloseHook();
  }

  private void setupCloseHook() {
    Context ctx = Vertx.currentContext();
    if (ctx != null && ctx.owner() == vertx) {
      ctx.addCloseHook(this::close);
    }
  }

  @Override
  public void close() {
    close(null);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    if (raiseCloseFlag()) {
      do {
        DataSourceHolder current = holders.get(datasourceName);
        DataSourceHolder next = current.decrement();
        if (next.refCount == 0) {
          if (holders.remove(datasourceName, current)) {
            if (current.dataSource != null) {
              doClose(current, completionHandler);
              return;
            }
            break;
          }
        } else if (holders.replace(datasourceName, current, next)) {
          break;
        }
      } while (true);
    }
    if (completionHandler != null) {
      completionHandler.handle(Future.succeededFuture());
    }
  }


  private synchronized boolean raiseCloseFlag() {
    if (!closed) {
      closed = true;
      return true;
    }
    return false;
  }

  @Override
  public JDBCClient update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
    ContextInternal ctx = vertx.getOrCreateContext();
    executeDirect(ctx, new JDBCUpdate(vertx, helper, null, ctx, sql, null), resultHandler);
    return this;
  }

  @Override
  public JDBCClient updateWithParams(String sql, JsonArray in, Handler<AsyncResult<UpdateResult>> resultHandler) {
    ContextInternal ctx = vertx.getOrCreateContext();
    executeDirect(ctx, new JDBCUpdate(vertx, helper, null, ctx, sql, in), resultHandler);
    return this;
  }

  @Override
  public JDBCClient query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    ContextInternal ctx = vertx.getOrCreateContext();
    executeDirect(ctx, new JDBCQuery(vertx, helper, null, ctx, sql, null), resultHandler);
    return this;
  }

  @Override
  public JDBCClient queryWithParams(String sql, JsonArray in, Handler<AsyncResult<ResultSet>> resultHandler) {
    ContextInternal ctx = vertx.getOrCreateContext();
    executeDirect(ctx, new JDBCQuery(vertx, helper, null, ctx, sql, in), resultHandler);
    return this;
  }

  private <T> void executeDirect(ContextInternal ctx, AbstractJDBCAction<T> action, Handler<AsyncResult<T>> handler) {
    getConnection(ctx, ar1 -> {
      Promise<T> promise = Promise.promise();
      promise.future().setHandler(ar2 -> ctx.runOnContext(v -> handler.handle(ar2)));
      if (ar1.succeeded()) {
        JDBCConnectionImpl conn = (JDBCConnectionImpl) ar1.result();
        conn.schedule(action, ar3 -> {
          if (ar3.succeeded()) {
            promise.complete(ar3.result());
          } else {
            promise.fail(ar3.cause());
          }
          if (conn.metrics != null) {
            conn.metrics.end(conn.metric, true);
          }
          try {
            conn.conn.close();
          } catch (Exception e) {
            JDBCConnectionImpl.log.error("Failure in closing connection", ar1.cause());
          }
        });
      } else {
        promise.fail(ar1.cause());
      }
    });
  }

  private void getConnection(ContextInternal ctx, Handler<AsyncResult<SQLConnection>> handler) {
    getDataSourceHolder(ctx, ar -> {
      if (ar.succeeded()) {
        DataSourceHolder holder = ar.result();
        boolean enabled = holder.metrics != null;
        Object queueMetric = enabled ? holder.metrics.submitted() : null;
        holder.exec.execute(() -> {
          Promise<SQLConnection> res = Promise.promise();
          res.future().setHandler(handler);
          try {
            /*
             * This can block until a connection is free.
             * We don't want to do that while running on a worker as we can enter a deadlock situation as the worker
             * might have obtained a connection, and won't release it until it is run again
             * There is a general principle here:
             * *User code* should be executed on a worker and can potentially block, it's up to the *user* to deal with
             * deadlocks that might occur there.
             * If the *service code* internally blocks waiting for a resource that might be obtained by *user code*, then
             * this can cause deadlock, so the service should ensure it never does this, by executing such code
             * (e.g. getConnection) on a different thread to the worker pool.
             * We don't want to use the vert.x internal pool for this as the threads might end up all blocked preventing
             * other important operations from occurring (e.g. async file access)
             */
            Connection conn = holder.dataSource.getConnection();
            Object execMetric = enabled ? holder.metrics.begin(queueMetric) : null;
            // wrap it
            res.complete(new JDBCConnectionImpl(ctx, helper, conn, holder.metrics, execMetric));
          } catch (SQLException e) {
            if (enabled) {
              holder.metrics.rejected(queueMetric);
            }
            res.fail(e);
          }
        });
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private synchronized void getDataSourceHolder(ContextInternal ctx, Handler<AsyncResult<DataSourceHolder>> handler) {
    if (closed) {
      handler.handle(Future.failedFuture("Client is closed"));
    } else {
      DataSourceHolder holder = holders.get(datasourceName);
      if (holder.dataSource != null) {
        handler.handle(Future.succeededFuture(holder));
      } else {
        ctx.executeBlocking(promise -> {
          createDataSource(promise);
        }, holder.creationQueue, handler);
      }
    }
  }

  private void createDataSource(Promise<DataSourceHolder> promise) {
    DataSourceHolder current = holders.get(datasourceName);
    if (current == null) {
      promise.fail("Client closed while connecting");
      return;
    }
    if (current.dataSource != null) {
      promise.complete(current);
      return;
    }
    DataSourceProvider provider = current.provider;
    DataSource dataSource;
    int poolSize;
    try {
      dataSource = provider.getDataSource(config);
      poolSize = provider.maximumPoolSize(dataSource, config);
    } catch (SQLException e) {
      promise.fail(e);
      return;
    }
    ExecutorService exec = createExecutor();
    PoolMetrics metrics = createMetrics(datasourceName, poolSize);
    current = holders.compute(datasourceName, (k, h) -> h == null ? null : h.created(dataSource, exec, metrics));
    if (current != null) {
      promise.complete(current);
    } else {
      if (metrics != null) {
        metrics.close();
      }
      exec.shutdown();
      try {
        provider.close(dataSource);
      } catch (SQLException ignored) {
      }
      promise.fail("Client closed while connecting");
    }
  }

  @Override
  public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
    ContextInternal ctx = vertx.getOrCreateContext();
    getConnection(ctx, ar -> ctx.runOnContext(v -> handler.handle(ar)));
    return this;
  }

  private DataSourceProvider createProvider() {
    String providerClass = config.getString("provider_class");
    if (providerClass == null) {
      providerClass = JDBCClient.DEFAULT_PROVIDER_CLASS;
    }

    if (Thread.currentThread().getContextClassLoader() != null) {
      try {
        // Try with the TCCL
        Class clazz = Thread.currentThread().getContextClassLoader().loadClass(providerClass);
        return (DataSourceProvider) clazz.newInstance();
      } catch (ClassNotFoundException e) {
        // Next try.
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      // Try with the classloader of the current class.
      Class clazz = this.getClass().getClassLoader().loadClass(providerClass);
      return (DataSourceProvider) clazz.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private PoolMetrics createMetrics(String poolName, int maxPoolSize) {
    VertxMetrics metricsSPI = vertx.metricsSPI();
    return metricsSPI != null ? metricsSPI.createPoolMetrics("datasource", poolName, maxPoolSize) : null;
  }

  private ExecutorService createExecutor() {
    return new ThreadPoolExecutor(1, 1,
      1000L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<>(),
      (r -> new Thread(r, "vertx-jdbc-service-get-connection-thread")));
  }

  private void doClose(DataSourceHolder holder, Handler<AsyncResult<Void>> completionHandler) {
    if (holder.metrics != null) {
      holder.metrics.close();
    }
    vertx.<Void>executeBlocking(promise -> {
      try {
        if (holder.provider != null) {
          holder.provider.close(holder.dataSource);
        }
        promise.complete();
      } catch (SQLException e) {
        promise.fail(e);
      }
    }, false, ar -> {
      holder.exec.shutdown();
      if (completionHandler != null) {
        completionHandler.handle(Future.succeededFuture());
      }
    });
  }
}
