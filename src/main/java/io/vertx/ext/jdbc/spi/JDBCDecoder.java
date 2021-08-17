package io.vertx.ext.jdbc.spi;

import io.vertx.ext.jdbc.impl.actions.JDBCTypeProvider;
import io.vertx.ext.jdbc.impl.actions.SQLValueProvider;

import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Represents for JDBC decoder from SQL value to Java value
 * <p>
 * The default decoder provides the best efforts to convert {@code SQL type} to standard {@code Java type} as {@code JDBC 4.2} spec.
 * <p>
 * You can replace it to adapt to a specific SQL driver by creating your owns then includes in the SPI file
 * ({@code META-INF/services/io.vertx.ext.jdbc.spi.JDBCDecoder})
 *
 * @see <a href=https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/jdbc_42.html">Mapping of java.sql.Types to SQL types</a>
 * @see io.vertx.ext.jdbc.spi.impl.JDBCDecoderImpl
 * @see java.sql.JDBCType
 * @see java.sql.SQLType
 * @since 4.2.0
 */
public interface JDBCDecoder {

  /**
   * Parse SQL value to Java value
   *
   * @param rs             JDBC result set
   * @param pos            the Database column position
   * @param jdbcTypeLookup JDBCType provider
   * @return java value
   * @throws SQLException if any error in parsing
   * @see ResultSet
   * @see JDBCTypeProvider
   */
  Object parse(ResultSet rs, int pos, JDBCTypeProvider jdbcTypeLookup) throws SQLException;

  /**
   * Parse SQL value to Java value
   *
   * @param cs             JDBC callable statement
   * @param pos            the parameter column position
   * @param jdbcTypeLookup JDBCType provider
   * @return java value
   * @throws SQLException if any error in parsing
   * @see CallableStatement
   * @see JDBCTypeProvider
   */
  Object parse(CallableStatement cs, int pos, JDBCTypeProvider jdbcTypeLookup) throws SQLException;

  /**
   * Convert the SQL value to Java value based on jdbc type
   *
   * @param jdbcType      JDBC type
   * @param valueProvider the value provider
   * @return java value
   * @see SQLValueProvider
   */
  Object decode(JDBCType jdbcType, SQLValueProvider valueProvider) throws SQLException;

  /**
   * Try cast SQL value to standard Java value depends on standard JDBC 4.2 type mapping and compatible with Vertx
   * <p>
   * For example:
   * - java.sql.Time -> java.time.LocalTime
   * - java.sql.Timestamp -> java.time.LocalDateTime
   *
   * @param value value
   * @return a presenter value
   * @throws SQLException if any error when casting
   */
  Object cast(Object value) throws SQLException;

}
