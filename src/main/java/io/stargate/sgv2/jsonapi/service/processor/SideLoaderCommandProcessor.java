package io.stargate.sgv2.jsonapi.service.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.CommandLevelLoggingConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.FileWriterSession;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

public class SideLoaderCommandProcessor {

  private final String namespace;
  private final String collection;

  public SideLoaderCommandProcessor(String namespace, String collection) {
    this.namespace = namespace;
    this.collection = collection;
  }

  public String beginWriterSession() {
    String sessionId = UUID.randomUUID().toString();
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(
            Optional.of(sessionId), new FileWriterParams(this.namespace, this.collection));
    OperationsConfig operationsConfig = buildOperationsConfig();
    MeterRegistry meterRegistry = buildMeterRegistry();
    CqlSession fileWriterSession =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, meterRegistry).getSession();
    if (fileWriterSession == null) {
      throw new RuntimeException("Can not create SSTable writer session");
    }
    return sessionId;
  }

  public SSTableWriterStatus insertDocuments(String sessionId, List<JsonNode> documents)
      throws ExecutionException, InterruptedException {
    OperationsConfig operationsConfig = buildOperationsConfig();
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(
            Optional.of(sessionId), new FileWriterParams(this.namespace, this.collection));
    MeterRegistry meterRegistry = buildMeterRegistry();
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, meterRegistry);
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
    MeteredCommandProcessor meteredCommandProcessor =
        getMeteredCommandProcessor(queryExecutor, meterRegistry, dataApiRequestInfo);
    // Build command and command context
    FileWriterSession fileWriterSession = (FileWriterSession) cqlSessionCache.getSession();
    CommandContext commandContext =
        new CommandContext(fileWriterSession.getNamespace(), fileWriterSession.getCollection());
    InsertManyCommand.Options options = new InsertManyCommand.Options(false);
    Command insertManyCommand = new InsertManyCommand(documents, options);
    // Execute command
    CommandResult commandResult =
        meteredCommandProcessor
            .processCommand(commandContext, insertManyCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    // Convert result to SSTableWriterStatus
    return toSSTableWriterStatus(commandContext, commandResult);
  }

  private static MeteredCommandProcessor getMeteredCommandProcessor(
      QueryExecutor queryExecutor,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo) {
    CommandResolverService commandResolverService = buildCommandResolverService();
    CommandProcessor commandProcessor = new CommandProcessor(queryExecutor, commandResolverService);
    JsonApiMetricsConfig jsonApiMetricsConfig = buildJsonApiMetricsConfig();
    MetricsConfig metricsConfig = buildMetricsConfig();
    CommandLevelLoggingConfig commandLevelLoggingConfig = buildCommandLevelLoggingConfig();
    return new MeteredCommandProcessor(
        commandProcessor,
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        metricsConfig,
        commandLevelLoggingConfig);
  }

  private static SSTableWriterStatus toSSTableWriterStatus(
      CommandContext commandContext, CommandResult commandResult) {
    return new SSTableWriterStatus(commandContext.namespace(), commandContext.collection());
  }

  public SSTableWriterStatus getWriterStatus(String sessionId) {
    FileWriterSession fileWriterSession =
        (FileWriterSession) new CQLSessionCache(null, null, null).getSession();
    return fileWriterSession.getStatus();
  }

  public void endWriterSession(String sessionId) {
    CqlSession fileWriterSession = new CQLSessionCache(null, null, null).getSession();
    fileWriterSession.close();
  }

  private static OperationsConfig buildOperationsConfig() {
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

  private static MeterRegistry buildMeterRegistry() {
    return new PrometheusMeterRegistry(null); // TODO
  }

  private static CommandResolverService buildCommandResolverService() {
    return null; // TODO
  }

  private static JsonApiMetricsConfig buildJsonApiMetricsConfig() {
    return null; // TODO
  }

  private static CommandLevelLoggingConfig buildCommandLevelLoggingConfig() {
    return null; // TODO
  }

  private static MetricsConfig buildMetricsConfig() {
    return null; // TODO
  }
}
