package io.vertx.ext.jdbc.impl.actions;

import java.sql.SQLException;

@FunctionalInterface
public interface SQLValueProvider {

  Object apply(Class cls) throws SQLException;

}
