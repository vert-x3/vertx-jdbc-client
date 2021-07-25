package io.vertx.ext.jdbc.spi;

import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Represents for JDBC decoder
 * <p>
 * The default decoder provides best efforts to convert {@code SQL type} to standard {@code Java} type as JDBC 4.2.
 * <p>
 * You can replace it to adapt to an specific SQL driver by creating your owns then includes in the SPI file
 * ({@code META-INF/services/io.vertx.ext.jdbc.spi.JDBCDecoder})
 *
 * @see <a href=https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/jdbc_42.html">Mapping of java.sql.Types to SQL types</a>
 * @see io.vertx.ext.jdbc.spi.impl.JDBCDecoderImpl
 * @see java.sql.JDBCType
 * @see java.sql.SQLType
 * @since 4.2.0
 */
public interface JDBCDecoder extends JDBCParser {

  /**
   * Parse SQL value to Java value
   *
   * @param metaData JDBC result set metadata
   * @param pos      the Database column position
   * @param rs       JDBC result set
   * @return java value
   * @throws SQLException if any error in parsing
   * @see ResultSetMetaData
   * @see ResultSet
   */
  Object parse(ResultSetMetaData metaData, int pos, ResultSet rs) throws SQLException;

  /**
   * Parse SQL value to Java value
   *
   * @param metaData JDBC parameter meta data
   * @param pos      the parameter column position
   * @param cs       JDBC callable statement
   * @return java value
   * @throws SQLException if any error in parsing
   * @see ParameterMetaData
   * @see CallableStatement
   */
  Object parse(ParameterMetaData metaData, int pos, CallableStatement cs) throws SQLException;

  /**
   * Convert SQL value to java value based on jdbc type
   *
   * @param jdbcType      JDBC type
   * @param valueProvider the value provider
   * @return java value
   * @see SQLValueProvider
   */
  Object convert(JDBCType jdbcType, SQLValueProvider valueProvider) throws SQLException;

  /**
   * Try cast SQL value to standard java depends on JDBC type mapping
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

  @FunctionalInterface
  interface SQLValueProvider {

    Object apply(Class cls) throws SQLException;

  }
}
