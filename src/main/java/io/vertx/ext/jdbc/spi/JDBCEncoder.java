package io.vertx.ext.jdbc.spi;

import java.sql.JDBCType;
import java.sql.SQLException;

/**
 * Represents for JDBC encoder from Java value to SQL value
 * <p>
 * The default encoder provides the best efforts to convert {@code Java type} to {@code SQL type} as {@code JDBC 4.2} spec.
 * <p>
 * You can replace it to adapt to a specific SQL driver by creating your owns then includes in the SPI file
 * ({@code META-INF/services/io.vertx.ext.jdbc.spi.JDBCEncoder})
 *
 * @see <a href=https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/jdbc_42.html">Mapping of java.sql.Types to SQL types</a>
 * @see io.vertx.ext.jdbc.spi.impl.JDBCEncoderImpl
 * @see java.sql.JDBCType
 * @see java.sql.SQLType
 * @since 4.2.0
 */
public interface JDBCEncoder {

  JDBCEncoder setup(boolean castUUID, boolean castDate, boolean castTime, boolean castDatetime);

  Object convert(JDBCType jdbcType, Object javaValue) throws SQLException;

}
