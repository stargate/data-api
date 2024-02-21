package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.CommandLevelLoggingConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

public class SideloaderConfigHelper {
  public static DocumentLimitsConfig buildDocumentLimitsConfig() {
    return null; // TODO
  }

  public static MeterRegistry buildMeterRegistry() {
    return new PrometheusMeterRegistry(null); // TODO
  }

  public static JsonApiMetricsConfig buildJsonApiMetricsConfig() {
    return null; // TODO
  }

  public static CommandLevelLoggingConfig buildCommandLevelLoggingConfig() {
    return null; // TODO
  }

  public static MetricsConfig buildMetricsConfig() {
    return null; // TODO
  }

  public static OperationsConfig buildOperationsConfig() {
    return new OperationsConfig() {
      @Override
      public int defaultPageSize() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int defaultSortPageSize() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int maxDocumentSortCount() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int maxDocumentDeleteCount() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int maxDocumentUpdateCount() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int maxDocumentInsertCount() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int maxFilterObjectProperties() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int maxInOperatorValueSize() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int maxVectorSearchLimit() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int maxCountLimit() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public int defaultCountPageSize() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public LwtConfig lwt() {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Nullable
      @Override
      public String ssTaableDefinition() {
        throw new UnsupportedOperationException("Not implemented"); // TODO
      }

      @Override
      public DatabaseConfig databaseConfig() {
        return new DatabaseConfig() {
          @Override
          public String type() {
            return CQLSessionCache.SIDE_LOADER;
          }

          @Nullable
          @Override
          public String userName() {
            throw new UnsupportedOperationException("Not implemented");
          }

          @Nullable
          @Override
          public String password() {
            throw new UnsupportedOperationException("Not implemented");
          }

          @Override
          public Optional<String> fixedToken() {
            throw new UnsupportedOperationException("Not implemented");
          }

          @Nullable
          @Override
          public List<String> cassandraEndPoints() {
            throw new UnsupportedOperationException("Not implemented");
          }

          @Nullable
          @Override
          public int cassandraPort() {
            throw new UnsupportedOperationException("Not implemented");
          }

          @Override
          public String localDatacenter() {
            throw new UnsupportedOperationException("Not implemented");
          }

          @Override
          public long sessionCacheTtlSeconds() {
            throw new UnsupportedOperationException("Not implemented");
          }

          @Override
          public int sessionCacheMaxSize() {
            return 100; // TODO
          }

          @Override
          public int ddlRetryDelayMillis() {
            throw new UnsupportedOperationException("Not implemented");
          }

          @Override
          public int ddlDelayMillis() {
            throw new UnsupportedOperationException("Not implemented");
          }
        };
      }

      @Override
      public QueriesConfig queriesConfig() {
        throw new UnsupportedOperationException("Not implemented");
      }
    };
  }
}
