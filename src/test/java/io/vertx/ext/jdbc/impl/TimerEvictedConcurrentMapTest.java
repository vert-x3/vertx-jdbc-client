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
    map = new TimerEvictedConcurrentMap<>(vertx, 100);
  }

  @Test
  public void testPut() {
    map.put("foo", "bar");
    vertx.setTimer(150, id -> {
      assertFalse(map.containsKey("foo"));
      testComplete();
    });

    await();
  }

  @Test
  public void testListener() {
    map.addEvictionListener((key, value) -> {
      assertEquals("foo", key);
      assertEquals("bar", value);
      testComplete();
    });
    map.put("foo", "bar");

    await();
  }

  @Test
  public void testGet() {
    map.put("foo", "bar");

    map.addEvictionListener((key, value) -> {
      fail("Should not have been evicted");
    });

    waitFor(3);
    vertx.setPeriodic(50, id -> {
      assertEquals("bar", map.get("foo"));
      complete();
    });

    await();
  }
}
