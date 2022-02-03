package io.vertx.ext.jdbc.impl.actions;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CachedParameterMetaData implements ParameterMetaData {

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
                isNullable = delegate.isNullable(param);
            }
            return isNullable;
        }

        boolean isSigned() throws SQLException {
            if (isSigned == null) {
                isSigned = delegate.isSigned(param);
            }
            return isSigned;
        }

        int getPrecision() throws SQLException {
            if (getPrecision == null) {
                getPrecision = delegate.getPrecision(param);
            }
            return getPrecision;
        }

        int getScale() throws SQLException {
            if (getScale == null) {
                getScale = delegate.getScale(param);
            }
            return getScale;
        }

        int getParameterType() throws SQLException {
            if (getParameterType == null) {
                getParameterType = delegate.getParameterType(param);
            }
            return getParameterType;
        }

        String getParameterTypeName() throws SQLException {
            if (getParameterTypeName == null) {
                getParameterTypeName = delegate.getParameterTypeName(param);
            }
            return getParameterTypeName;
        }

        String getParameterClassName() throws SQLException {
            if (getParameterClassName == null) {
                getParameterClassName = delegate.getParameterClassName(param);
            }
            return getParameterClassName;
        }

        int getParameterMode() throws SQLException {
            if (getParameterMode == null) {
                getParameterMode = delegate.getParameterMode(param);
            }
            return getParameterMode;
        }
    }

    private final ParameterMetaData delegate;
    private final Map<Integer, QueryMeta> queryMetaMap = new HashMap<>();

    public CachedParameterMetaData(ParameterMetaData delegate) {
        this.delegate = delegate;
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
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }
}
