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
 * = Vert.x JDBC Service
 *
 * The {@link io.vertx.ext.jdbc.JdbcService JDBC Service} is responsible for providing an interface for Vert.x applications that need to interact with
 * a database using a JDBC driver.
 *
 * == Getting Started
 *
 * Since the JDBC service is a service proxy, there are several ways to get started, but the easiest is to just deploy the service
 * verticle:
 * [source,{lang}]
 * ----
 * {@link examples.io.vertx.ext.jdbc.Examples#deployJdbcService()}
 * ----
 *
 * and get the service like so:
 * [source,{lang}]
 * ----
 * {@link examples.io.vertx.ext.jdbc.Examples#eventBusProxy()}
 * ----
 *
 * You can also specify configuration options during the deployment.
 * [source,{lang}]
 * ----
 * {@link examples.io.vertx.ext.jdbc.Examples#deployJdbcServiceWithConfig()}
 * ----
 *
 * This example configures the JDBC service to use a postgres JDBC url in oder to connect to a postgresql database. As long
 * as the postgresql JDBC driver is on the classpath, the service will be able to connect and interact with the postgresql database.
 *
 * //TODO: complete docs
 */
@Document(fileName = "index.adoc")
@GenModule(name = "vertx-jdbc")
package io.vertx.ext.jdbc;

import io.vertx.codegen.annotations.GenModule;
import io.vertx.docgen.Document;