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

import io.vertx.core.Vertx;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class TimerEvictedConcurrentMap<K, V> {

  private final Vertx vertx;
  private final long defaultTimeout;
  private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();
  private final List<Consumer<K>> evictionListeners = new CopyOnWriteArrayList<>();

  public TimerEvictedConcurrentMap(Vertx vertx) {
    this(vertx, -1);
  }

  public TimerEvictedConcurrentMap(Vertx vertx, long defaultTimeout) {
    this.vertx = vertx;
    this.defaultTimeout = defaultTimeout;
  }

  public void addEvictionListener(Consumer<K> listener) {
    evictionListeners.add(listener);
  }

  public V get(K key) {
    return map.get(key);
  }

  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  public V put(K key, V value) {
    return put(key, value, defaultTimeout);
  }

  public V put(K key, V value, long timeout) {
    V v = map.put(key, value);
    startEviction(key, timeout);
    return v;
  }

  public V putIfAbsent(K key, V value) {
    return putIfAbsent(key, value, defaultTimeout);
  }

  public V putIfAbsent(K key, V value, long timeout) {
    V v = map.putIfAbsent(key, value);
    if (v != null) {
      startEviction(key, timeout);
    }
    return v;
  }

  private void startEviction(final K key, final long timeout) {
    if (timeout > 0) {
      vertx.setTimer(timeout, id -> {
        map.remove(key);
        for (Consumer<K> listener : evictionListeners) {
          listener.accept(key);
        }
      });
    }
  }
}
