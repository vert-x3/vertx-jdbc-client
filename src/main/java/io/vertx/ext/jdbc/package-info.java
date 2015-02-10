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
/**
 * = Vert.x JDBC service
 *
 * This service allows you to interact with any JDBC compliant database using an asynchronous API from your Vert.x
 * application.
 *
 * == Setting up the service
 *
 * As with other services you can use the service either by deploying it as a verticle somewhere on your network and
 * interacting with it over the event bus, either directly by sending messages, or using a service proxy, e.g.
 *
 * Somewhere you deploy it:
 *
 * [source,java]
 * ----
 * {@link examples.Examples#example1}
 * ----
 *
 * Now you can either send messages to it directly over the event bus, or you can create a proxy to the service
 * from wherever you are and just use that:
 *
 * [source,java]
 * ----
 * {@link examples.Examples#example2}
 * ----
 *
 * Alternatively you can create an instance of the service directly and just use that locally:
 *
 * [source,java]
 * ----
 * {@link examples.Examples#example3}
 * ----
 *
 * If you create an instance this way you should make sure you start it with {@link io.vertx.ext.jdbc.JdbcService#start}
 * before you use it.
 *
 * You can also create a local instance specifying a Java datasource:
 *
 * [source,java]
 * ----
 * {@link examples.Examples#example3_1}
 * ----
 *
 * However you do it, once you've got your service you can start using it.
 *
 * == Getting a connection
 *
 * Use {@link io.vertx.ext.jdbc.JdbcService#getConnection(io.vertx.core.Handler)} to get a connection.
 *
 * This will return the connection in the handler when one is ready from the pool.
 *
 * [source,java]
 * ----
 * {@link examples.Examples#example4}
 * ----
 *
 * Once you've finished with the connection make sure you close it afterwards.
 *
 * The connection is an instance of {@link io.vertx.ext.sql.SqlConnection} which is a common interface used by
 * more than Vert.x sql service.
 *
 * You can learn how to use it in the http://foobar[common sql interface] documentation.
 *
 * == Configuration
 *
 * Configuration is passed to the service when creating or deploying it.
 *
 * The following configuration properties generally apply:
 *
 * `address`:: The address this service should register on the event bus. Defaults to `vertx.jdbc`.
 * `provider_class`:: The class name of the class actually used to manage the database connections. By default this is
 * `io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider`but if you want to use a different provider you can override
 * this property and provide your implementation.
 *
 * Assuming the C3P0 implementation is being used (the default), the following extra configuration properties apply:
 *
 * `url`:: the JDBC connection URL for the database
 * `driver_class`:: the class of the JDBC driver
 * `user`:: the username for the database
 * `password`:: the password for the database
 * `max_pool_size`:: the maximum number of connections to pool - default is `15`
 * `initial_pool_size`:: the number of connections to initialise the pool with - default is `3`
 * `min_pool_size`:: the minimum number of connections to pool
 * `max_statements`:: the maximum number of prepared statements to cache - default is `0`.
 * `max_statements_per_connection`:: the maximum number of prepared statements to cache per connection - default is `0`.
 * `max_idle_time`:: number of seconds after which an idle connection will be closed - default is `0` (never expire).
 *
 * If you want to configure any other C3P0 properties, you can add a file `c3p0.properties` to the classpath.
 *
 * Here's an example of configuring a service:
 *
 * [source,java]
 * ----
 * {@link examples.Examples#example5}
 * ----
 *
 */
@Document(fileName = "index.adoc")
@GenModule(name = "vertx-jdbc")
package io.vertx.ext.jdbc;

import io.vertx.codegen.annotations.GenModule;
import io.vertx.docgen.Document;