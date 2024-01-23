package io.vertx.jdbcclient.impl.actions;

import io.vertx.jdbcclient.spi.JDBCColumnDescriptorProvider;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.RowDesc;

import java.sql.SQLException;

public class JDBCRowDesc extends RowDesc {

  private static ColumnDescriptor[] foo(JDBCColumnDescriptorProvider provider, int cols) throws SQLException {
    ColumnDescriptor[] columnDescriptors = new ColumnDescriptor[cols];
    for (int i = 0; i < cols; i++) {
      JDBCColumnDescriptor columnDescriptor = provider.apply(i + 1);
      columnDescriptors[i] = columnDescriptor;
    }
    return columnDescriptors;
  }

  public JDBCRowDesc() {
    super(new ColumnDescriptor[0]);
  }

  public JDBCRowDesc(ColumnDescriptor[] columnDescriptors) {
    super(columnDescriptors);
  }

  public JDBCRowDesc(JDBCColumnDescriptorProvider provider, int cols) throws SQLException {
    super(foo(provider, cols));
  }
}
