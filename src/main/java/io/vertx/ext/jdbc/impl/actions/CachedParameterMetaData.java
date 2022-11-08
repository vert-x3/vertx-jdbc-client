package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;

public class CachedParameterMetaData implements ParameterMetaData {

  private static final Logger LOG = LoggerFactory.getLogger(CachedParameterMetaData.class);


  class QueryMeta {
    private final int param;
    private Integer isNullable;
    private Boolean isSigned;
    private Integer getPrecision;
    private Integer getScale;
    private Integer getParameterType;
    private String getParameterTypeName;
    private String getParameterClassName;
    private Integer getParameterMode;

    QueryMeta(int param) {
      this.param = param;
    }

    int isNullable() throws SQLException {
      if (isNullable == null) {
        if (delegate == null) {
          throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
        }
        isNullable = delegate.isNullable(param);
      }
      return isNullable;
    }

    boolean isSigned() throws SQLException {
      if (delegate == null) {
        throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
      }
      if (isSigned == null) {
        isSigned = delegate.isSigned(param);
      }
      return isSigned;
    }

    int getPrecision() throws SQLException {
      if (getPrecision == null) {
        if (delegate == null) {
          throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
        }
        getPrecision = delegate.getPrecision(param);
      }
      return getPrecision;
    }

    int getScale() throws SQLException {
      if (getScale == null) {
        if (delegate == null) {
          throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
        }
        getScale = delegate.getScale(param);
      }
      return getScale;
    }

    int getParameterType() throws SQLException {
      if (getParameterType == null) {
        if (delegate == null) {
          throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
        }
        getParameterType = delegate.getParameterType(param);
      }
      return getParameterType;
    }

    String getParameterTypeName() throws SQLException {
      if (getParameterTypeName == null) {
        if (delegate == null) {
          throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
        }
        getParameterTypeName = delegate.getParameterTypeName(param);
      }
      return getParameterTypeName;
    }

    String getParameterClassName() throws SQLException {
      if (getParameterClassName == null) {
        if (delegate == null) {
          throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
        }
        getParameterClassName = delegate.getParameterClassName(param);
      }
      return getParameterClassName;
    }

    int getParameterMode() throws SQLException {
      if (getParameterMode == null) {
        if (delegate == null) {
          throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
        }
        getParameterMode = delegate.getParameterMode(param);
      }
      return getParameterMode;
    }
  }

  private final ParameterMetaData delegate;
  private final Map<Integer, QueryMeta> queryMetaMap = new HashMap<>();
  private final CallableOutParams outParams = CallableOutParams.create();

  public CachedParameterMetaData(PreparedStatement statement) {
    ParameterMetaData metaData;
    try {
      metaData = statement.getParameterMetaData();
    } catch (SQLFeatureNotSupportedException e) {
      // OK, not really but we can deal with it...
      metaData = null;
    } catch (SQLException e) {
      // the correct way probably would be the catch the operation not supported, but not all drivers
      // report the specific exception
      LOG.debug("Driver doesn't support getParameterMetaData()", e);
      metaData = null;
    }

    this.delegate = metaData;
  }

  public ParameterMetaData putOutParams(CallableOutParams outParams) {
    if (outParams != null) {
      this.outParams.putAll(outParams);
    }
    return this;
  }

  private QueryMeta getQueryMeta(int param) {
    QueryMeta meta = queryMetaMap.get(param);
    if (meta == null) {
      meta = new QueryMeta(param);
      queryMetaMap.put(param, meta);
    }
    return meta;
  }

  @Override
  public int getParameterCount() throws SQLException {
    if (delegate == null) {
      throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
    }
    return delegate.getParameterCount();
  }

  @Override
  public int isNullable(int param) throws SQLException {
    //noinspection MagicConstant
    return getQueryMeta(param).isNullable();
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    return getQueryMeta(param).isSigned();
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    return getQueryMeta(param).getPrecision();
  }

  @Override
  public int getScale(int param) throws SQLException {
    return getQueryMeta(param).getScale();
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    if (outParams.containsKey(param)) {
      return outParams.get(param).vendorTypeNumber();
    }
    return getQueryMeta(param).getParameterType();
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    return getQueryMeta(param).getParameterTypeName();
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    return getQueryMeta(param).getParameterClassName();
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
    //noinspection MagicConstant
    return getQueryMeta(param).getParameterMode();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (delegate == null) {
      throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
    }
    return delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (delegate == null) {
      throw new SQLFeatureNotSupportedException("getParameterMetaData() unsupported by JDBC driver");
    }
    return delegate.isWrapperFor(iface);
  }
}
