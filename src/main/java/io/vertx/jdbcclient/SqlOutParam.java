package io.vertx.jdbcclient;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.jdbcclient.impl.SqlOutParamImpl;

import java.sql.JDBCType;

@VertxGen
public interface SqlOutParam {

  static SqlOutParam OUT(int out) {
    return new SqlOutParamImpl(out);
  }

  static SqlOutParam OUT(String out) {
    return new SqlOutParamImpl(JDBCType.valueOf(out).getVendorTypeNumber());
  }

  static SqlOutParam OUT(JDBCType out) {
    return new SqlOutParamImpl(out.getVendorTypeNumber());
  }

  static SqlOutParam INOUT(Object in, int out) {
    return new SqlOutParamImpl(in, out);
  }

  static SqlOutParam INOUT(Object in, String out) {
    return new SqlOutParamImpl(in, JDBCType.valueOf(out).getVendorTypeNumber());
  }

  static SqlOutParam INOUT(Object in, JDBCType out) {
    return new SqlOutParamImpl(in, out.getVendorTypeNumber());
  }

  boolean in();

  int type();

  Object value();
}
