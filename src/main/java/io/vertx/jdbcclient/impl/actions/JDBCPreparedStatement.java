package io.vertx.jdbcclient.impl.actions;

import io.vertx.sqlclient.impl.ParamDesc;
import io.vertx.sqlclient.impl.PreparedStatement;
import io.vertx.sqlclient.impl.RowDesc;
import io.vertx.sqlclient.impl.TupleInternal;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JDBCPreparedStatement implements PreparedStatement {

  private final String sql;
  private final java.sql.PreparedStatement preparedStatement;
  private final RowDesc rowDesc;
  private final ParamDesc paramDesc;

  public JDBCPreparedStatement(String sql, java.sql.PreparedStatement preparedStatement) throws SQLException {

    List<String> columnNames = new ArrayList<>();
    ResultSetMetaData metaData = preparedStatement.getMetaData();
    if (metaData != null) {
      // Not a SELECT
      int cols = metaData.getColumnCount();
      for (int i = 1; i <= cols; i++) {
        columnNames.add(metaData.getColumnLabel(i));
      }
    }
    // apply statement options
    // applyStatementOptions(statement);

    this.sql = sql;
    this.rowDesc = new RowDesc(columnNames);
    this.paramDesc = new ParamDesc();
    this.preparedStatement = preparedStatement;
  }

  public java.sql.PreparedStatement preparedStatement() {
    return preparedStatement;
  }

  @Override
  public ParamDesc paramDesc() {
    return paramDesc;
  }

  @Override
  public RowDesc rowDesc() {
    return rowDesc;
  }

  @Override
  public String sql() {
    return sql;
  }

  @Override
  public String prepare(TupleInternal values) {
    return null;
  }
}
