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
import io.vertx.core.net.SocketAddress;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCClientImpl implements JDBCClient, Closeable {

  private static String parseNetLocation(String host) {
    if (isRegardedAsIpv6Address(host)) {
      return host.substring(1, host.length() - 1);
    } else {
      return host;
    }
  }

  private static boolean isRegardedAsIpv6Address(String address) {
    return address.startsWith("[") && address.endsWith("]");
  }

  private static final String NET_LOCATION_REGEX = "(?<netloc>[0-9.]+|\\[[a-zA-Z0-9:]+]|[a-zA-Z0-9\\-._~%]+)"; // ip v4/v6 address, host, domain socket address
  private static final String PORT_REGEX = "(:(?<port>\\d+))?"; // port
  private static final Pattern HOST_AND_PORT_PATTERN = Pattern.compile("://" + NET_LOCATION_REGEX + PORT_REGEX);
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
    DataSourceProvider provider = DataSourceProvider.create(config);
    holders.compute(datasourceName, (k, h) -> h == null ? new DataSourceHolder(provider) : h.increment());
    this.helper = new JDBCStatementHelper(config);
    setupCloseHook();
  }

  /**
   * Create client with shared datasource.
   */
  public JDBCClientImpl(Vertx vertx, DataSourceProvider dataSourceProvider) {
    this(
      vertx,
      dataSourceProvider,
      dataSourceProvider.getInitialConfig().getString("datasourceName", UUID.randomUUID().toString())
    );
  }

  /**
   * Create client with shared datasource.
   */
  public JDBCClientImpl(Vertx vertx, DataSourceProvider dataSourceProvider, final String datasourceName) {
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(dataSourceProvider);

    this.vertx = (VertxInternal) vertx;
    this.datasourceName = datasourceName;
    this.config = dataSourceProvider.getInitialConfig();
    holders = vertx.sharedData().getLocalMap(DS_LOCAL_MAP_NAME);
    holders.compute(datasourceName, (k, h) -> h == null ? new DataSourceHolder(dataSourceProvider) : h.increment());
    this.helper = new JDBCStatementHelper(this.config);
    setupCloseHook();
  }

  private void setupCloseHook() {
    ContextInternal ctx = vertx.getContext();
    if (ctx != null) {
      ctx.addCloseHook(this);
    }
  }

  public JDBCStatementHelper getHelper() {
    return helper;
  }

  @Override
  public void close(Promise<Void> completion) {
    close((Handler<AsyncResult<Void>>) completion);
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
      if (completionHandler != null) {
        completionHandler.handle(Future.succeededFuture());
      }
    } else if (completionHandler != null) {
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
    executeDirect(new JDBCUpdate(helper, null, sql, null), resultHandler);
    return this;
  }

  @Override
  public JDBCClient updateWithParams(String sql, JsonArray in, Handler<AsyncResult<UpdateResult>> resultHandler) {
    executeDirect(new JDBCUpdate(helper, null, sql, in), resultHandler);
    return this;
  }

  @Override
  public JDBCClient query(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
    executeDirect(new JDBCQuery(helper, null, sql, null), resultHandler);
    return this;
  }

  @Override
  public JDBCClient queryWithParams(String sql, JsonArray in, Handler<AsyncResult<ResultSet>> resultHandler) {
    executeDirect(new JDBCQuery(helper, null, sql, in), resultHandler);
    return this;
  }

  private <T> void executeDirect(AbstractJDBCAction<T> action, Handler<AsyncResult<T>> handler) {
    getConnection().flatMap(sqlConnection -> {
      JDBCConnectionImpl conn = (JDBCConnectionImpl) sqlConnection;
      return conn.schedule(action).onComplete(v -> conn.close());
    }).onComplete(handler);
  }

  public Future<SQLConnection> getConnection() {
    return getConnection(vertx.getOrCreateContext());
  }

  public Future<SQLConnection> getConnection(ContextInternal ctx) {
    return getDataSourceHolder(ctx).flatMap(holder -> {
      Promise<SQLConnection> res = ctx.promise();
      boolean enabled = holder.metrics != null;
      Object queueMetric = enabled ? holder.metrics.submitted() : null;
      holder.exec.execute(() -> {
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
          String url = conn.getMetaData().getURL();
          SocketAddress server;
          Matcher match = HOST_AND_PORT_PATTERN.matcher(url);
          if (match.find()) {
            String host = parseNetLocation(match.group("netloc"));
            int port;
            String portString;
            if (match.groupCount() > 1 && (portString = match.group("port")) != null && portString.length() > 0) {
              port = Integer.parseInt(portString);
            } else {
              port = 0;
            }
            server = SocketAddress.inetSocketAddress(port, host);
          } else  {
            // Use a dummy address when cannot guess one
            server = SocketAddress.inetSocketAddress(1234, "unknown");
          }
          res.complete(new JDBCConnectionImpl(ctx, helper, conn, holder.metrics, execMetric, server));
        } catch (SQLException e) {
          if (enabled) {
            holder.metrics.rejected(queueMetric);
          }
          res.fail(e);
        }
      });
      return res.future();
    });
  }

  private synchronized Future<DataSourceHolder> getDataSourceHolder(ContextInternal ctx) {
    if (closed) {
      return ctx.failedFuture("Client is closed");
    }
    DataSourceHolder holder = holders.get(datasourceName);
    if (holder.dataSource != null) {
      return ctx.succeededFuture(holder);
    }
    return ctx.executeBlocking(promise -> {
      createDataSource(promise);
    }, holder.creationQueue);
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
    getConnection().onComplete(handler);
    return this;
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
