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
import java.util.function.BiConsumer;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class TimerEvictedConcurrentMap<K, V> {

  private final Vertx vertx;
  private final long timeout;
  private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();
  private final List<BiConsumer<K, V>> evictionListeners = new CopyOnWriteArrayList<>();
  private final ConcurrentMap<K, Long> timers = new ConcurrentHashMap<>();

  public TimerEvictedConcurrentMap(Vertx vertx, long timeout) {
    this.vertx = vertx;
    this.timeout = timeout;
  }

  public void addEvictionListener(BiConsumer<K, V> listener) {
    evictionListeners.add(listener);
  }

  public V get(K key) {
    V value = map.get(key);
    Long id = timers.remove(key);
    if (id != null) {
      vertx.cancelTimer(id);
      startEviction(key, value);
    }

    return value;
  }

  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  public V put(K key, V value) {
    V v = map.put(key, value);
    startEviction(key, value);
    return v;
  }

  public V putIfAbsent(K key, V value) {
    V v = map.putIfAbsent(key, value);
    if (v == null) {
      startEviction(key, value);
    }
    return v;
  }

  public V remove(K key) {
    V value = map.get(key);
    Long id = timers.remove(key);
    if (id != null) {
      vertx.cancelTimer(id);
    }

    return value;
  }

  private void startEviction(final K key, final V value) {
    if (timeout > 0) {
      Long timerId = vertx.setTimer(timeout, id -> {
        map.remove(key);
        timers.remove(key);
        for (BiConsumer<K, V> listener : evictionListeners) {
          listener.accept(key, value);
        }
      });
      timers.put(key, timerId);
    }
  }
}
