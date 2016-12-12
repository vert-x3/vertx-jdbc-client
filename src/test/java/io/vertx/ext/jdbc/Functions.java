package io.vertx.ext.jdbc;

import java.sql.*;

public class Functions {

  public static int nap(int howLong) {
    try {
      Thread.sleep(howLong * 1000);
    } catch (InterruptedException e) {
      return -1;
    }
    return howLong;
  }

  public static void multiSelect(ResultSet[] data1, ResultSet[] data2) throws SQLException {

    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    PreparedStatement ps1 = conn.prepareStatement("select * from SYS.SYSTABLES");
    data1[0] = ps1.executeQuery();

    PreparedStatement ps2 = conn.prepareStatement("select * from SYS.SYSTABLES");
    data2[0] = ps2.executeQuery();

    conn.close();
  }
}
