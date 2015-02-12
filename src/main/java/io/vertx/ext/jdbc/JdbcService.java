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

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.ProxyIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.JdbcServiceImpl;
import io.vertx.ext.sql.SqlConnection;
import io.vertx.serviceproxy.ProxyHelper;

import javax.sql.DataSource;

/**
 * A JDBC service allows you to interact with a JDBC compatible database using an asynchronous API.
 *
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
@VertxGen
@ProxyGen
public interface JdbcService {


  /**
   * Create a service locally
   *
   * @param vertx  the Vert.x instance
   * @param config  the configuration
   * @return the service
   */
  static JdbcService create(Vertx vertx, JsonObject config) {
    return new JdbcServiceImpl(vertx, config, null);
  }

  /**
   * Create an event bus proxy to a service which lives somewhere on the network and is listening on the specified
   * event bus address
   *
   * @param vertx  the Vert.x instance
   * @param address  the address on the event bus where the service is listening
   * @return
   */
  static JdbcService createEventBusProxy(Vertx vertx, String address) {
    return ProxyHelper.createProxy(JdbcService.class, vertx, address);
  }

  /**
   * Create a service using a pre-existing datasource
   *
   * @param vertx  the Vert.x instance
   * @param config  the configuration
   * @param dataSource  the datasource
   * @return the service
   */
  @GenIgnore
  static JdbcService create(Vertx vertx, JsonObject config, DataSource dataSource) {
    return new JdbcServiceImpl(vertx, config, dataSource);
  }

  /**
   * Returns a connection that can be used to perform SQL operations on. It's important to remember
   * to close the connection when you are done, so it is returned to the pool.
   *
   * @param handler the handler which is called when the <code>JdbcConnection</code> object is ready for use.
   */
  void getConnection(Handler<AsyncResult<SqlConnection>> handler);

  /**
   * Start the service
   */
  @ProxyIgnore
  public void start();

  /**
   * Stop the service
   */
  @ProxyIgnore
  public void stop();
}
