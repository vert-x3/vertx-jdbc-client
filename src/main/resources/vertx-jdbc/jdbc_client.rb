require 'vertx/vertx'
require 'vertx-sql/sql_connection'
require 'vertx/util/utils.rb'
# Generated from io.vertx.ext.jdbc.JDBCClient
module VertxJdbc
  #  An asynchronous client interface for interacting with a JDBC compliant database
  class JDBCClient
    # @private
    # @param j_del [::VertxJdbc::JDBCClient] the java delegate
    def initialize(j_del)
      @j_del = j_del
    end
    # @private
    # @return [::VertxJdbc::JDBCClient] the underlying java delegate
    def j_del
      @j_del
    end
    #  Create a JDBC client which maintains its own data source.
    # @param [::Vertx::Vertx] vertx the Vert.x instance
    # @param [Hash{String => Object}] config the configuration
    # @return [::VertxJdbc::JDBCClient] the client
    def self.create_non_shared(vertx=nil,config=nil)
      if vertx.class.method_defined?(:j_del) && config.class == Hash && !block_given?
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxExtJdbc::JDBCClient.java_method(:createNonShared, [Java::IoVertxCore::Vertx.java_class,Java::IoVertxCoreJson::JsonObject.java_class]).call(vertx.j_del,::Vertx::Util::Utils.to_json_object(config)),::VertxJdbc::JDBCClient)
      end
      raise ArgumentError, "Invalid arguments when calling create_non_shared(vertx,config)"
    end
    #  Create a JDBC client which shares its data source with any other JDBC clients created with the same
    #  data source name
    # @param [::Vertx::Vertx] vertx the Vert.x instance
    # @param [Hash{String => Object}] config the configuration
    # @param [String] dataSourceName the data source name
    # @return [::VertxJdbc::JDBCClient] the client
    def self.create_shared(vertx=nil,config=nil,dataSourceName=nil)
      if vertx.class.method_defined?(:j_del) && config.class == Hash && !block_given? && dataSourceName == nil
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxExtJdbc::JDBCClient.java_method(:createShared, [Java::IoVertxCore::Vertx.java_class,Java::IoVertxCoreJson::JsonObject.java_class]).call(vertx.j_del,::Vertx::Util::Utils.to_json_object(config)),::VertxJdbc::JDBCClient)
      elsif vertx.class.method_defined?(:j_del) && config.class == Hash && dataSourceName.class == String && !block_given?
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxExtJdbc::JDBCClient.java_method(:createShared, [Java::IoVertxCore::Vertx.java_class,Java::IoVertxCoreJson::JsonObject.java_class,Java::java.lang.String.java_class]).call(vertx.j_del,::Vertx::Util::Utils.to_json_object(config),dataSourceName),::VertxJdbc::JDBCClient)
      end
      raise ArgumentError, "Invalid arguments when calling create_shared(vertx,config,dataSourceName)"
    end
    #  Returns a connection that can be used to perform SQL operations on. It's important to remember
    #  to close the connection when you are done, so it is returned to the pool.
    # @yield the handler which is called when the <code>JdbcConnection</code> object is ready for use.
    # @return [self]
    def get_connection
      if block_given?
        @j_del.java_method(:getConnection, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ::Vertx::Util::Utils.safe_create(ar.result,::VertxSql::SQLConnection) : nil) }))
        return self
      end
      raise ArgumentError, "Invalid arguments when calling get_connection()"
    end
    #  Close the client
    # @return [void]
    def close
      if !block_given?
        return @j_del.java_method(:close, []).call()
      end
      raise ArgumentError, "Invalid arguments when calling close()"
    end
  end
end
