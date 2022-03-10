/*
 * Copyright (c) 2011-2022 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.jdbcclient.data;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.format.SnakeCase;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.annotations.Column;
import io.vertx.sqlclient.templates.annotations.ParametersMapped;
import io.vertx.sqlclient.templates.annotations.RowMapped;

@DataObject(generateConverter = true)
@RowMapped(formatter = SnakeCase.class)
@ParametersMapped(formatter = SnakeCase.class)
public class MyObject {
  public enum Status {
    ABC
  }

  @Column(name = "ID")
  private int id;

  @Column(name = "STATUS")
  private Status type;

  public MyObject() {
  }

  public MyObject(MyObject other) {
    id = other.id;
    type = other.type;
  }

  public MyObject(JsonObject json) {
    MyObjectConverter.fromJson(json, this);
  }

  public int getId() {
    return id;
  }

  public MyObject setId(int id) {
    this.id = id;
    return this;
  }

  public Status getType() {
    return type;
  }

  public MyObject setType(Status type) {
    this.type = type;
    return this;
  }
}
