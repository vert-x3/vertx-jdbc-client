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
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.jdbc.JdbcService;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.sql.SqlConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JdbcServiceImpl implements JdbcService {

  private static final Logger log = LoggerFactory.getLogger(JdbcService.class);

  private final Vertx vertx;
  private final JsonObject config;
  private DataSourceProvider provider;
  private DataSource dataSource;
  // We use this executor to execute getConnection requests
  private ExecutorService exec;

  public JdbcServiceImpl(Vertx vertx, JsonObject config, DataSource dataSource) {
    this.vertx = vertx;
    this.config = config;
    this.dataSource = dataSource;
  }

  @Override
  public void start() {
    if (dataSource == null) {
      String providerClass = config.getString("provider_class");
      if (providerClass == null) {
        // Default to C3P0
        providerClass = "io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider";
      }
      try {
        Class clazz = getClassLoader().loadClass(providerClass);
        provider = (DataSourceProvider) clazz.newInstance();
        dataSource = provider.getDataSource(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    exec = new ThreadPoolExecutor(1, 1,
                                  1000L, TimeUnit.MILLISECONDS,
                                  new LinkedBlockingQueue<>(),
                                  (r -> new Thread(r, "vertx-jdbc-service-get-connection-thread")));
  }

  @Override
  public void stop() {
    if (exec != null) {
      exec.shutdown();
    }
    if (provider != null) {
      vertx.executeBlocking(future -> {
        try {
          provider.close(dataSource);
          future.complete();
        } catch (SQLException e) {
          future.fail(e);
        }
      }, null);
    }
  }

  @Override
  public void getConnection(Handler<AsyncResult<SqlConnection>> handler) {
    Context ctx = vertx.getOrCreateContext();
    exec.execute(() -> {
      Future<SqlConnection> res = Future.future();
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
        Connection conn = dataSource.getConnection();
        SqlConnection sconn = new JdbcConnectionImpl(vertx, conn);
        res.complete(sconn);
      } catch (SQLException e) {
        res.fail(e);
      }
      ctx.runOnContext(v -> res.setHandler(handler));
    });
  }

  private ClassLoader getClassLoader() {
    ClassLoader tccl = Thread.currentThread().getContextClassLoader();
    return tccl == null ? getClass().getClassLoader() : tccl;
  }
}
