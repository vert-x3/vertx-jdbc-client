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

import io.vertx.test.core.VertxTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class TimerEvictedConcurrentMapTest extends VertxTestBase {

  private TimerEvictedConcurrentMap<String, String> map;

  @Before
  public void init() {
    map = new TimerEvictedConcurrentMap<>(vertx);
  }

  @Test
  public void testPutWithTimeout() {
    map.put("foo", "bar", 50);
    vertx.setTimer(100, id -> {
      assertFalse(map.containsKey("foo"));
      testComplete();
    });

    await();
  }

  @Test
  public void testPutWithNegativeTimeout() {
    map.put("foo", "bar", -1);
    vertx.setTimer(100, id -> {
      assertTrue(map.containsKey("foo"));
      testComplete();
    });

    await();
  }

  @Test
  public void testPutWithLargeNegativeTimeout() {
    map.put("foo", "bar", -100); // should be the same as passing in -1
    vertx.setTimer(100, id -> {
      assertTrue(map.containsKey("foo"));
      testComplete();
    });

    await();
  }

  @Test
  public void testPutWithZeroTimeout() {
    map.put("foo", "bar", 0); // should be the same as passing in -1
    vertx.setTimer(100, id -> {
      assertTrue(map.containsKey("foo"));
      testComplete();
    });

    await();
  }

  @Test
  public void testPutWithDefaultTimeout() {
    map = new TimerEvictedConcurrentMap<>(vertx, 50);
    map.put("foo", "bar");
    vertx.setTimer(100, id -> {
      assertFalse(map.containsKey("foo"));
      testComplete();
    });

    await();
  }

  @Test
  public void testListener() {
    map.addEvictionListener(key -> {
      assertEquals("foo", key);
      testComplete();
    });
    map.put("foo", "bar", 50);

    await();
  }
}
