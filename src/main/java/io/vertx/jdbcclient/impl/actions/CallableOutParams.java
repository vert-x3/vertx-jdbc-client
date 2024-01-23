package io.vertx.jdbcclient.impl.actions;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;

public interface CallableOutParams extends Map<Integer, JDBCTypeWrapper> {

  static CallableOutParams create() {
    return new CallableOutParamsImpl();
  }

  default JDBCTypeWrapper put(Integer key, Integer sqlType) {
    return this.put(key, JDBCTypeWrapper.of(sqlType));
  }

  default JDBCTypeWrapper put(Integer key, String jdbcTypeName) {
    return this.put(key, JDBCTypeWrapper.of(jdbcTypeName));
  }

  default JDBCTypeWrapper put(Integer key, JDBCType jdbcType) {
    return this.put(key, JDBCTypeWrapper.of(jdbcType));
  }

  class CallableOutParamsImpl extends HashMap<Integer, JDBCTypeWrapper> implements CallableOutParams {

  }
}
