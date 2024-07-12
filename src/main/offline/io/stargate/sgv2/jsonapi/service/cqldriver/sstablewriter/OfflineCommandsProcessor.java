package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.response.BeginOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.EndOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineGetStatusResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineInsertManyResponse;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.BeginOfflineSessionCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.EndOfflineSessionCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineGetStatusCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineInsertManyCommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OfflineCommandsProcessor {
  private static final Logger logger = LoggerFactory.getLogger(OfflineCommandsProcessor.class);
  private final OperationsConfig operationsConfig;
  private final CQLSessionCache cqlSessionCache;
  private final CommandResolverService commandResolverService;
  private final DataVectorizerService dataVectorizerService;
  private static OfflineCommandsProcessor offlineCommandsProcessor;
  private static boolean initialized;

  private OfflineCommandsProcessor() {
    OfflineFileWriterInitializer.initialize();
    operationsConfig = OfflineFileWriterInitializer.buildOperationsConfig();
    cqlSessionCache = buildCqlSessionCache(operationsConfig);
    commandResolverService = buildCommandResolverService();
    dataVectorizerService = buildDataVectorizeService();
  }

  public static OfflineCommandsProcessor getInstance() {
    if (!initialized) {
      init();
    }
    return offlineCommandsProcessor;
  }

  // TODO-SL see if init can be removed
  private static synchronized void init() {
    if (initialized) {
      return;
    }
    offlineCommandsProcessor = new OfflineCommandsProcessor();
    initialized = true;
  }

  private static CQLSessionCache buildCqlSessionCache(OperationsConfig operationsConfig) {
    return new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
  }

  private CommandResolverService buildCommandResolverService() {
    ObjectMapper objectMapper = new ObjectMapper();
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder().withMapping(DocumentLimitsConfig.class).build();
    DocumentLimitsConfig documentLimitsConfig =
        smallRyeConfig.getConfigMapping(DocumentLimitsConfig.class);
    Shredder shredder = new Shredder(objectMapper, documentLimitsConfig, null);
    return new CommandResolverService(
        List.of(
            new BeginOfflineSessionCommandResolver(),
            new EndOfflineSessionCommandResolver(),
            new OfflineGetStatusCommandResolver(),
            new OfflineInsertManyCommandResolver(shredder, this.operationsConfig)));
  }

  private static DataVectorizerService buildDataVectorizeService() {
    ObjectMapper objectMapper = new ObjectMapper();
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(DocumentLimitsConfig.class)
            .withMapping(OperationsConfig.class)
            .withMapping(MetricsConfig.class)
            .build();
    return new DataVectorizerService(objectMapper, new SimpleMeterRegistry(), null);
  }

  public OfflineGetStatusResponse getStatus(CommandContext commandContext, String sessionId)
      throws ExecutionException, InterruptedException {
    CommandProcessor commandProcessor =
        new CommandProcessor(
            new QueryExecutor(cqlSessionCache, operationsConfig),
            commandResolverService,
            dataVectorizerService);
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo(Optional.of(sessionId));
    OfflineGetStatusCommand offlineGetStatusCommand = new OfflineGetStatusCommand(sessionId);
    CommandResult commandResult =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineGetStatusCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    return OfflineGetStatusResponse.fromCommandResult(commandResult);
  }

  public boolean canEndSession(
      OfflineWriterSessionStatus offlineWriterSessionStatus,
      long createNewSessionAfterDataInBytes) {
    return offlineWriterSessionStatus.dataDirectorySizeInBytes()
        >= createNewSessionAfterDataInBytes;
  }

  public Triple<BeginOfflineSessionResponse, CommandContext, SchemaInfo> beginSession(
      CreateCollectionCommand createCollectionCommand,
      String namespace,
      String ssTablesOutputDirectory,
      int fileWriterBufferSizeInMB,
      EmbeddingProvider embeddingProvider)
      throws ExecutionException, InterruptedException {
    CommandProcessor commandProcessor =
        new CommandProcessor(
            new QueryExecutor(cqlSessionCache, operationsConfig),
            commandResolverService,
            dataVectorizerService);
    BeginOfflineSessionCommand beginOfflineSessionCommand =
        new BeginOfflineSessionCommand(
            namespace, createCollectionCommand, ssTablesOutputDirectory, fileWriterBufferSizeInMB);

    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(Optional.of(beginOfflineSessionCommand.getSessionId()));

    CommandContext commandContext =
        CommandContext.from(
            namespace,
            createCollectionCommand.name(),
            beginOfflineSessionCommand.getCollectionSettings(),
            embeddingProvider,
            beginOfflineSessionCommand.getClass().getSimpleName());
    CommandResult commandResult =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, beginOfflineSessionCommand)
            .onFailure()
            .invoke(t -> logger.error("Exception while beginning session", t))
            .subscribe()
            .asCompletionStage()
            .get();
    BeginOfflineSessionResponse beginOfflineSessionResponse =
        BeginOfflineSessionResponse.fromCommandResult(commandResult);
    return new ImmutableTriple<>(
        beginOfflineSessionResponse,
        commandContext,
        new SchemaInfo(
            beginOfflineSessionCommand.getFileWriterParams().keyspaceName(),
            beginOfflineSessionCommand.getFileWriterParams().tableName(),
            beginOfflineSessionCommand.getFileWriterParams().createTableCQL(),
            beginOfflineSessionCommand.getFileWriterParams().indexCQLs()));
  }

  public OfflineInsertManyResponse loadData(
      String sessionId, CommandContext commandContext, List<JsonNode> records)
      throws ExecutionException, InterruptedException {
    CommandProcessor commandProcessor =
        new CommandProcessor(
            new QueryExecutor(cqlSessionCache, operationsConfig),
            commandResolverService,
            dataVectorizerService);
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo(Optional.of(sessionId));
    // TODO - SL, what happens if some documents fail ?
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(sessionId, records);
    CommandResult commandResult =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineInsertManyCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    return OfflineInsertManyResponse.fromCommandResult(commandResult);
  }

  public EndOfflineSessionResponse endSession(String sessionId, CommandContext commandContext)
      throws ExecutionException, InterruptedException {
    CommandProcessor commandProcessor =
        new CommandProcessor(
            new QueryExecutor(cqlSessionCache, operationsConfig),
            commandResolverService,
            OfflineCommandsProcessor.buildDataVectorizeService());
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo(Optional.of(sessionId));
    EndOfflineSessionCommand offlineEndWriterCommand = new EndOfflineSessionCommand(sessionId);
    CommandResult commandResult =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineEndWriterCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    return EndOfflineSessionResponse.fromCommandResult(commandResult);
  }
}
