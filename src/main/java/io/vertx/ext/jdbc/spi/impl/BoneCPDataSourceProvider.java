/*
* Copyright (c) 2011-2015 The original author or authors
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

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class BoneCPDataSourceProvider implements DataSourceProvider {

  private static final Logger log = LoggerFactory.getLogger(BoneCPDataSourceProvider.class);

  /**
   * Since BoneCP has way to many knobs and due to the fact that this method is called only
   * once per client instance it inspects the config object using reflection.
   */
  private static void mergeConfig(BoneCPConfig config, JsonObject json) {

    for (Map.Entry<String, Object> entry : json) {
      String name = entry.getKey();

      if ("provider_class".equals(name)) {
        continue;
      }

      String mName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
      try {
        Method method = BoneCPConfig.class.getMethod(mName, entry.getValue().getClass());
        method.invoke(config, entry.getValue());
      } catch (NoSuchMethodException e) {
        // ignore, means that the config object does not have this property
        log.warn("no such property: " + name);
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private ClassLoader getClassLoader() {
    ClassLoader tccl = Thread.currentThread().getContextClassLoader();
    return tccl == null ? getClass().getClassLoader() : tccl;
  }

  @Override
  public DataSource getDataSource(JsonObject config) throws SQLException {
    BoneCPConfig boneCPConfig = new BoneCPConfig();
    mergeConfig(boneCPConfig, config);

    // we now need to change the default class loader mostly for the cases where
    // we do not use fat jars. In that case other connection pools will fail
    // loading the drivers
    boneCPConfig.setClassLoader(getClassLoader());

    return new BoneCPDataSource(boneCPConfig);
  }

  @Override
  public int maximumPoolSize(DataSource dataSource, JsonObject config) {
    if (dataSource instanceof BoneCPDataSource) {
      BoneCPConfig cfg = ((BoneCPDataSource) dataSource).getPool().getConfig();
      return cfg.getMaxConnectionsPerPartition() * cfg.getPartitionCount();
    }
    return -1;
  }

  @Override
  public void close(DataSource dataSource) throws SQLException {
    if (dataSource instanceof BoneCPDataSource) {
      ((BoneCPDataSource) dataSource).close();
    }
  }
}
