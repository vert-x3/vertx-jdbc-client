package io.vertx.ext.jdbc.spi.impl;

 import com.alibaba.druid.Constants;
 import com.alibaba.druid.pool.DruidDataSource;
 import com.alibaba.druid.stat.JdbcDataSourceStat;
 import io.vertx.core.json.JsonObject;
 import io.vertx.core.logging.Logger;
 import io.vertx.core.logging.LoggerFactory;
 import io.vertx.ext.jdbc.spi.DataSourceProvider;
 import javax.sql.DataSource;
 import java.sql.SQLException;
 import java.util.*;

/**
  * add support for alibaba druid drivers by Issacui
  */
 public class DruidDataSourceProvider implements DataSourceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DruidDataSourceProvider.class);
   @Override
   public DataSource getDataSource(JsonObject config)  {
       DruidDataSource ds = new DruidDataSource();

     for (Map.Entry<String, Object> entry : config) {
         configFromPropety(ds,entry);
     }
     return ds;
   }

   @Override
   public void close(DataSource dataSource)  {
     if (dataSource instanceof DruidDataSource) {
       ((DruidDataSource) dataSource).close();
     }
   }

   @Override
   public int maximumPoolSize(DataSource dataSource, JsonObject config)  {
     if (dataSource instanceof DruidDataSource) {
       return ((DruidDataSource) dataSource).getMaxActive();
     }
     return -1;
   }



    private void configFromPropety(DruidDataSource ds ,Map.Entry<String, Object> entry) {
       String key = entry.getKey();
        String value = null;
        if(entry.getValue()!=null){value = entry.getValue().toString().trim();}
        if("provider_class".equalsIgnoreCase(key)){return;}
        if("name".equalsIgnoreCase(key)){
            if (value != null) {
                ds.setName(( value).trim());
            }
        }else if ("url".equalsIgnoreCase(key))
        {
            if (value != null) {
                ds.setUrl(( value).trim());
            }
        }else if("username".equalsIgnoreCase(key))
        {
            if (value != null) {
                ds.setUsername(( value).trim());
            }
        }else if("password".equalsIgnoreCase(key))
        {
            if (value != null) {
                ds.setPassword(( value).trim());
            }
        }else  if("testWhileIdle".equalsIgnoreCase(key))
        {
            if (value != null) {
                ds.setTestWhileIdle(Boolean.parseBoolean(value));
            }
        }else  if("testOnBorrow".equalsIgnoreCase(key))
        {
            if (value != null) {
                ds.setTestOnBorrow(Boolean.parseBoolean(value));
            }
        }else  if("validationQuery".equalsIgnoreCase(key))
        {
            if (value != null && (value).length() > 0) {
                ds.setValidationQuery(( value).trim());
            }
        } else if ("validationQueryTimeout".equalsIgnoreCase(key)) {
            if(value!=null && value.length()>0){
                ds.setValidationQueryTimeout(Integer.parseInt(value));
            }
        } else if ("useGlobalDataSourceStat".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setUseGlobalDataSourceStat(Boolean.parseBoolean(value));
            }
        } else if ("useGloalDataSourceStat".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setUseGlobalDataSourceStat(Boolean.parseBoolean(value));
            }
        } else if ("asyncInit".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setAsyncInit(Boolean.parseBoolean(value));
            }
        } else if ("filters".equalsIgnoreCase(key)) {

            if (value != null && ( value).length() > 0) {
                try {
                    ds.setFilters(value);
                } catch (SQLException e) {
                    LOG.error("setFilters error", e);
                    throw new RuntimeException(e);
                }
            }
        } else if (Constants.DRUID_TIME_BETWEEN_LOG_STATS_MILLIS.equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    long v = Long.parseLong( value);
                    ds.setTimeBetweenLogStatsMillis(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property '" + Constants.DRUID_TIME_BETWEEN_LOG_STATS_MILLIS + "'", e);
                }
            }
        } else if (Constants.DRUID_STAT_SQL_MAX_SIZE.equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    int v = Integer.parseInt( value);
                    JdbcDataSourceStat dataSourceStat = ds.getDataSourceStat();
                    if (dataSourceStat != null) {
                        dataSourceStat.setMaxSqlSize(v);
                    }
                } catch (NumberFormatException e) {
                    LOG.error("illegal property '" + Constants.DRUID_STAT_SQL_MAX_SIZE + "'", e);
                }
            }
        } else if ("clearFiltersEnable".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setClearFiltersEnable(Boolean.parseBoolean(value));
            }
        } else if ("resetStatEnable".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setResetStatEnable(Boolean.parseBoolean(value));
            }
        } else if ("notFullTimeoutRetryCount".equalsIgnoreCase(key)) {
            if (value != null && (value).length() > 0) {
                try {
                    int v = Integer.parseInt(( value).trim());
                    ds.setNotFullTimeoutRetryCount(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.notFullTimeoutRetryCount'", e);
                }
            }
        } else if ("timeBetweenEvictionRunsMillis".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    long v = Long.parseLong( value);
                    ds.setTimeBetweenEvictionRunsMillis(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.timeBetweenEvictionRunsMillis'", e);
                }
            }
        } else if ("maxWaitThreadCount".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    int v = Integer.parseInt( value);
                    ds.setMaxWaitThreadCount(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.maxWaitThreadCount'", e);
                }
            }
        } else if ("failFast".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setFailFast(Boolean.parseBoolean(value));
            }
        } else if ("phyTimeoutMillis".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    long v = Long.parseLong( value);
                    ds.setPhyTimeoutMillis(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.phyTimeoutMillis'", e);
                }
            }
        } else if ("minEvictableIdleTimeMillis".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    long v = Long.parseLong( value);
                    ds.setMinEvictableIdleTimeMillis(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.minEvictableIdleTimeMillis'", e);
                }
            }
        } else if ("maxEvictableIdleTimeMillis".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    long v = Long.parseLong( value);
                    ds.setMaxEvictableIdleTimeMillis(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.maxEvictableIdleTimeMillis'", e);
                }
            }
        } else if ("keepAlive".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setKeepAlive(Boolean.parseBoolean(value));
            }
        } else if ("poolPreparedStatements".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setPoolPreparedStatements(Boolean.parseBoolean(value));
            }
        } else if ("initVariants".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setInitVariants(Boolean.parseBoolean(value));
            }
        } else if ("initGlobalVariants".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setInitGlobalVariants(Boolean.parseBoolean(value));
            }
        } else if ("useUnfairLock".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setUseUnfairLock(Boolean.parseBoolean(value));
            }
        } else if ("driverClassName".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setDriverClassName(( value).trim());
            }
        } else if ("initialSize".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    int v = Integer.parseInt((( value)).trim());
                    ds.setInitialSize(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.initialSize'", e);
                }
            }
        } else if ("minIdle".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    int v = Integer.parseInt(( value).trim());
                    ds.setMinIdle(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.minIdle'", e);
                }
            }
        } else if ("maxActive".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    int v = Integer.parseInt(( value).trim());
                    ds.setMaxActive(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.maxActive'", e);
                }
            }
        } else if ("killWhenSocketReadTimeout".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setKillWhenSocketReadTimeout(Boolean.parseBoolean(value));
            }
        } else if ("connectProperties".equalsIgnoreCase(key)||"connectionProperties".equalsIgnoreCase(key)) {
            if (value != null) {
                ds.setConnectionProperties(value);
            }
        } else if ("maxPoolPreparedStatementPerConnectionSize".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    int v = Integer.parseInt( value);
                    ds.setMaxPoolPreparedStatementPerConnectionSize(v);
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.maxPoolPreparedStatementPerConnectionSize'", e);
                }
            }
        } else if ("initConnectionSqls".equalsIgnoreCase(key)) {
            if (value != null && ( value).length() > 0) {
                try {
                    StringTokenizer tokenizer = new StringTokenizer( value, ";");
                    ds.setConnectionInitSqls(Collections.list(tokenizer));
                } catch (NumberFormatException e) {
                    LOG.error("illegal property 'druid.initConnectionSqls'", e);
                }
            }
        } else if ("maxPoolPreparedStatementPerConnectionSize".equalsIgnoreCase(key)) {
            if (value != null && value.length() > 0) {
                ds.setMaxPoolPreparedStatementPerConnectionSize(Integer.parseInt(value));
            }
        } else if ("maxWait".equalsIgnoreCase(key)) {
            if (value != null && value.length() > 0) {
                ds.setMaxWait(Long.parseLong(value));
            }
        }else if("testOnReturn".equalsIgnoreCase(key)){
            if(value !=null){
                ds.setTestOnReturn(Boolean.parseBoolean(value));
            }
        }
//        else if("numTestsPerEvictionRun".equalsIgnoreCase(key)){
////            if(value != null && value.length()>0){
////                ds.setNumTestsPerEvictionRun();
////            }
//        }
        else if("exceptionSorter".equalsIgnoreCase(key)){
            if(value != null){
                try {
                    ds.setExceptionSorter(value);
                } catch (SQLException e) {
                    LOG.error(e);
                    throw new RuntimeException(e);
                }
            }
        }
//        else if("proxyFilters".equalsIgnoreCase(key)){
//            if(value != null){
//                ds.setProxyFilters();
//            }
//        }
        else if("removeAbandoned".equalsIgnoreCase(key)){
            if(value != null){
                ds.setRemoveAbandoned(Boolean.parseBoolean(value));
            }
        }else if("removeAbandonedTimeout".equalsIgnoreCase(key)){
            if(value != null && value.length()>0){
                ds.setRemoveAbandonedTimeout(Integer.parseInt(value));
            }
        }else if("logAbandoned".equalsIgnoreCase(key)){
            if(value != null){
                ds.setLogAbandoned(Boolean.parseBoolean(value));
            }
        }else if("defaultReadOnly".equalsIgnoreCase(key)){
            if(value != null){
                ds.setDefaultReadOnly(Boolean.parseBoolean(value));
            }
        }else {
            throw new RuntimeException("wrong db config:" + key);
        }
    }
 }
