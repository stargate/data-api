package io.stargate.sgv2.jsonapi.service.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.CommandLevelLoggingConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.FileWriterSession;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.SSTableWriterStatus;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.InsertManyCommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class SideLoaderCommandProcessor {
  private final String namespace;
  private final String collection;
  private final OperationsConfig operationsConfig;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;
  private final MetricsConfig metricsConfig;
  private final CommandLevelLoggingConfig commandLevelLoggingConfig;
  private final DocumentLimitsConfig documentLimitsConfig;
  private final boolean vectorEnabled;
  private final CollectionSettings.SimilarityFunction similarityFunction;
  private final int vectorSize;
  private final String comment;
  private final FileWriterParams fileWriterParams;

  public SideLoaderCommandProcessor(
      String namespace,
      String collection,
      boolean isVectorEnabled,
      CollectionSettings.SimilarityFunction similarityFunction,
      int vectorSize,
      String optionsSpecification,
      String ssTableOutputDirectory) {
    this.vectorEnabled = isVectorEnabled;
    this.similarityFunction = similarityFunction;
    this.vectorSize = vectorSize;
    this.namespace = namespace;
    this.collection = collection;
    this.comment = optionsSpecification; // TODO-SL handle vectorize and indexing json config
    String createTableCQL =
        CreateCollectionOperation.forCQL(this.vectorEnabled, this.vectorSize, this.comment)
            .getCreateTable(this.namespace, this.collection)
            .getQuery();
    String insertStatementCQL =
        InsertOperation.forCQL(new CommandContext(namespace, collection))
            .buildInsertQuery(this.vectorEnabled);
    this.fileWriterParams =
        new FileWriterParams(
            namespace,
            collection,
            ssTableOutputDirectory + "_" + namespace + "_" + collection,
            createTableCQL,
            insertStatementCQL);
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(OperationsConfig.class)
            .withMapping(JsonApiMetricsConfig.class)
            .withMapping(MetricsConfig.class)
            .withMapping(CommandLevelLoggingConfig.class)
            .withMapping(DocumentLimitsConfig.class)
            // TODO-SL increase cache expiry limit
            .withDefaultValue("stargate.jsonapi.operations.database-config.type", "sideloader")
            .build();
    this.operationsConfig = smallRyeConfig.getConfigMapping(OperationsConfig.class);
    this.jsonApiMetricsConfig = smallRyeConfig.getConfigMapping(JsonApiMetricsConfig.class);
    this.metricsConfig = smallRyeConfig.getConfigMapping(MetricsConfig.class);
    this.commandLevelLoggingConfig =
        smallRyeConfig.getConfigMapping(CommandLevelLoggingConfig.class);
    this.documentLimitsConfig = smallRyeConfig.getConfigMapping(DocumentLimitsConfig.class);
    this.meterRegistry = new SimpleMeterRegistry();
  }

  public String beginWriterSession() {
    String sessionId = UUID.randomUUID().toString();
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(Optional.of(sessionId), this.fileWriterParams);
    CqlSession fileWriterSession =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, meterRegistry).getSession();
    if (fileWriterSession == null) {
      throw new RuntimeException("Can not create SSTable writer session");
    }
    return sessionId;
  }

  public SSTableWriterStatus insertDocuments(String sessionId, List<JsonNode> documents)
      throws ExecutionException, InterruptedException {
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(Optional.of(sessionId), this.fileWriterParams);
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, meterRegistry);
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
    MeteredCommandProcessor meteredCommandProcessor =
        getMeteredCommandProcessor(queryExecutor, meterRegistry, dataApiRequestInfo);
    // Build command and command context
    FileWriterSession fileWriterSession = (FileWriterSession) cqlSessionCache.getSession();
    CommandContext commandContext =
        new CommandContext(
            fileWriterSession.getNamespace(),
            fileWriterSession.getCollection(),
            CollectionSettings.getCollectionSettings(
                this.collection,
                this.vectorEnabled,
                this.vectorSize,
                this.similarityFunction,
                this.comment,
                new ObjectMapper()),
            null);
    new CommandContext(fileWriterSession.getNamespace(), fileWriterSession.getCollection());
    InsertManyCommand.Options options = new InsertManyCommand.Options(true);
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

  private MeteredCommandProcessor getMeteredCommandProcessor(
      QueryExecutor queryExecutor,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo) {
    CommandResolverService commandResolverService = buildCommandResolverService();
    CommandProcessor commandProcessor = new CommandProcessor(queryExecutor, commandResolverService);
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
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(Optional.of(sessionId), this.fileWriterParams);
    FileWriterSession fileWriterSession =
        (FileWriterSession)
            new CQLSessionCache(dataApiRequestInfo, this.operationsConfig, this.meterRegistry)
                .getSession();
    return fileWriterSession.getStatus();
  }

  public void endWriterSession(String sessionId) {
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(Optional.of(sessionId), this.fileWriterParams);
    CqlSession fileWriterSession =
        new CQLSessionCache(dataApiRequestInfo, this.operationsConfig, this.meterRegistry)
            .getSession();
    fileWriterSession.close();
  }

  private CommandResolverService buildCommandResolverService() {
    ObjectMapper objectMapper = new ObjectMapper();
    Shredder shredder = new Shredder(objectMapper, documentLimitsConfig);
    return new CommandResolverService(
        List.of(new InsertManyCommandResolver(shredder, objectMapper)));
  }
}
