/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.groovy.ext.jdbc;
import groovy.transform.CompileStatic
import io.vertx.lang.groovy.InternalHelper
import io.vertx.groovy.ext.sql.SqlConnection
import io.vertx.groovy.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
/**
 * A JDBC service allows you to interact with a JDBC compatible database using an asynchronous API.
 *
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
@CompileStatic
public class JdbcService {
  final def io.vertx.ext.jdbc.JdbcService delegate;
  public JdbcService(io.vertx.ext.jdbc.JdbcService delegate) {
    this.delegate = delegate;
  }
  public Object getDelegate() {
    return delegate;
  }
  /**
   * Create a service locally
   *
   * @param vertx  the Vert.x instance
   * @param config  the configuration
   * @return the service
   */
  public static JdbcService create(Vertx vertx, Map<String, Object> config) {
    def ret= JdbcService.FACTORY.apply(io.vertx.ext.jdbc.JdbcService.create((io.vertx.core.Vertx)vertx.getDelegate(), config != null ? new io.vertx.core.json.JsonObject(config) : null));
    return ret;
  }
  /**
   * Create an event bus proxy to a service which lives somewhere on the network and is listening on the specified
   * event bus address
   *
   * @param vertx  the Vert.x instance
   * @param address  the address on the event bus where the service is listening
   * @return
   */
  public static JdbcService createEventBusProxy(Vertx vertx, String address) {
    def ret= JdbcService.FACTORY.apply(io.vertx.ext.jdbc.JdbcService.createEventBusProxy((io.vertx.core.Vertx)vertx.getDelegate(), address));
    return ret;
  }
  /**
   * Returns a connection that can be used to perform SQL operations on. It's important to remember
   * to close the connection when you are done, so it is returned to the pool.
   *
   * @param handler the handler which is called when the <code>JdbcConnection</code> object is ready for use.
   */
  public void getConnection(Handler<AsyncResult<SqlConnection>> handler) {
    this.delegate.getConnection(new Handler<AsyncResult<io.vertx.ext.sql.SqlConnection>>() {
      public void handle(AsyncResult<io.vertx.ext.sql.SqlConnection> event) {
        AsyncResult<SqlConnection> f
        if (event.succeeded()) {
          f = InternalHelper.<SqlConnection>result(new SqlConnection(event.result()))
        } else {
          f = InternalHelper.<SqlConnection>failure(event.cause())
        }
        handler.handle(f)
      }
    });
  }
  /**
   * Start the service
   */
  public void start() {
    this.delegate.start();
  }
  /**
   * Stop the service
   */
  public void stop() {
    this.delegate.stop();
  }

  static final java.util.function.Function<io.vertx.ext.jdbc.JdbcService, JdbcService> FACTORY = io.vertx.lang.groovy.Factories.createFactory() {
    io.vertx.ext.jdbc.JdbcService arg -> new JdbcService(arg);
  };
}
