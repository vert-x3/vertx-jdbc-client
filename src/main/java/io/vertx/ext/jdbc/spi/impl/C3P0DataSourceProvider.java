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

package io.vertx.ext.jdbc.spi.impl;

import com.mchange.v2.c3p0.DataSources;
import com.mchange.v2.c3p0.PooledDataSource;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class C3P0DataSourceProvider implements DataSourceProvider {
  @Override
  public DataSource getDataSource(JsonObject config) throws SQLException {
    String url = config.getString("url");
    if (url == null) throw new NullPointerException("url cannot be null");

    return DataSources.pooledDataSource(DataSources.unpooledDataSource(url));
  }

  @Override
  public void close(DataSource dataSource) throws SQLException {
    if (dataSource instanceof PooledDataSource) {
      ((PooledDataSource) dataSource).close();
    }
  }
}
