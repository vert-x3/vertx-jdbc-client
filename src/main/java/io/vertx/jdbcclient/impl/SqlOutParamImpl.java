package io.vertx.jdbcclient.impl;

import io.vertx.jdbcclient.SqlOutParam;

public class SqlOutParamImpl implements SqlOutParam {

  final Object value;
  final int type;
  final boolean in;

  public SqlOutParamImpl(Object value, int type) {
    this.value = value;
    this.type = type;
    in = true;
  }

  public SqlOutParamImpl(int type) {
    this.value = null;
    this.type = type;
    in = false;
  }

  @Override
  public boolean in() {
    return in;
  }

  @Override
  public int type() {
    return type;
  }

  @Override
  public Object value() {
    return value;
  }
}
