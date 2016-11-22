/*
 * Copyright 2016 Red Hat, Inc.
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

package io.vertx.ext.jdbc;

import io.vertx.core.json.JsonObject;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Thomas Segismont
 */
public class ConfigFactory {

  private static final AtomicInteger idGen = new AtomicInteger();

  public static JsonObject createConfigForH2() {
    return new JsonObject()
      .put("url", "jdbc:h2:mem:test-" + idGen.incrementAndGet() + ";DB_CLOSE_DELAY=-1")
      .put("driver_class", "org.h2.Driver");
  }

  private ConfigFactory() {
    // Utility class
  }
}
