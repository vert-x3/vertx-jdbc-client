package io.vertx.jdbcclient.impl.actions;

import java.sql.SQLException;

@FunctionalInterface
public interface SQLValueProvider {

  Object apply(Class cls) throws SQLException;

}
