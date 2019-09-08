/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.ext.jdbc.impl;

import io.vertx.core.impl.TaskQueue;
import io.vertx.core.shareddata.Shareable;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * @author Thomas Segismont
 */
class DataSourceHolder implements Shareable {

  final TaskQueue creationQueue;
  final DataSourceProvider provider;
  final DataSource dataSource;
  final ExecutorService exec;
  final PoolMetrics metrics;
  final int refCount;

  DataSourceHolder(DataSourceProvider provider) {
    this(new TaskQueue(), provider, null, null, null, 1);
  }

  DataSourceHolder(DataSource dataSource, ExecutorService exec, PoolMetrics metrics) {
    this(null, null, dataSource, exec, metrics, 1);
  }

  private DataSourceHolder(TaskQueue creationQueue, DataSourceProvider provider, DataSource dataSource, ExecutorService exec, PoolMetrics metrics, int refCount) {
    if (dataSource != null) {
      Objects.requireNonNull(exec);
    } else {
      Objects.requireNonNull(creationQueue);
      Objects.requireNonNull(provider);
    }
    this.creationQueue = creationQueue;
    this.provider = provider;
    this.dataSource = dataSource;
    this.exec = exec;
    this.metrics = metrics;
    this.refCount = refCount;
  }

  DataSourceHolder created(DataSource dataSource, ExecutorService exec, PoolMetrics metrics) {
    Objects.requireNonNull(dataSource);
    Objects.requireNonNull(exec);
    if (this.dataSource != null) {
      throw new IllegalStateException();
    }
    return new DataSourceHolder(creationQueue, provider, dataSource, exec, metrics, refCount);
  }

  DataSourceHolder increment() {
    return new DataSourceHolder(creationQueue, provider, dataSource, exec, metrics, refCount + 1);
  }

  DataSourceHolder decrement() {
    if (refCount < 1) {
      throw new IllegalArgumentException();
    }
    return new DataSourceHolder(creationQueue, provider, dataSource, exec, metrics, refCount - 1);
  }
}
