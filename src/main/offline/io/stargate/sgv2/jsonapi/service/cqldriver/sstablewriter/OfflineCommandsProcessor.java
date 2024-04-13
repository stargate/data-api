package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.OFFLINE_WRITER;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.response.BeginOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.EndOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineGetStatusResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineInsertManyResponse;
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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OfflineCommandsProcessor {
  private static final Logger logger = LoggerFactory.getLogger(OfflineCommandsProcessor.class);
  private OperationsConfig operationsConfig;
  private CQLSessionCache cqlSessionCache;
  private CommandResolverService commandResolverService;
  private DataVectorizerService dataVectorizerService;
  private static OfflineCommandsProcessor offlineCommandsProcessor;
  private static boolean initialized;

  private OfflineCommandsProcessor() {}

  public static OfflineCommandsProcessor getInstance() {
    if (!initialized) {
      init();
    }
    return offlineCommandsProcessor;
  }

  private static synchronized void init() {
    if (initialized) {
      return;
    }
    OfflineFileWriterInitializer.initialize();
    offlineCommandsProcessor = new OfflineCommandsProcessor();
    offlineCommandsProcessor.operationsConfig = buildOperationsConfig();
    offlineCommandsProcessor.cqlSessionCache =
        buildCqlSessionCache(offlineCommandsProcessor.operationsConfig);
    offlineCommandsProcessor.commandResolverService = buildCommandResolverService();
    offlineCommandsProcessor.dataVectorizerService = buildDataVectorizeService();
    initialized = true;
  }

  private static OperationsConfig buildOperationsConfig() {
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(OperationsConfig.class)
            // TODO-SL increase cache expiry limit
            .withDefaultValue("stargate.jsonapi.operations.database-config.type", OFFLINE_WRITER)
            .build();
    return smallRyeConfig.getConfigMapping(OperationsConfig.class);
  }

  private static CQLSessionCache buildCqlSessionCache(OperationsConfig operationsConfig) {
    return new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
  }

  private static CommandResolverService buildCommandResolverService() {
    ObjectMapper objectMapper = new ObjectMapper();
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder().withMapping(DocumentLimitsConfig.class).build();
    DocumentLimitsConfig documentLimitsConfig =
        smallRyeConfig.getConfigMapping(DocumentLimitsConfig.class);
    Shredder shredder = new Shredder(objectMapper, documentLimitsConfig, null);
    return new CommandResolverService(
        List.of(
            new BeginOfflineSessionCommandResolver(shredder, objectMapper),
            new EndOfflineSessionCommandResolver(shredder, objectMapper),
            new OfflineGetStatusCommandResolver(shredder, objectMapper),
            new OfflineInsertManyCommandResolver(shredder, objectMapper)));
  }

  private static DataVectorizerService buildDataVectorizeService() {
    ObjectMapper objectMapper = new ObjectMapper();
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(DocumentLimitsConfig.class)
            .withMapping(OperationsConfig.class)
            .withMapping(MetricsConfig.class)
            .build();
    MetricsConfig metricsConfig = smallRyeConfig.getConfigMapping(MetricsConfig.class);
    return new DataVectorizerService(objectMapper, new SimpleMeterRegistry(), null, metricsConfig);
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
      OfflineWriterSessionStatus offlineWriterSessionStatus, int createNewSessionAfterDataInMB) {
    ;
    return offlineWriterSessionStatus.dataDirectorySizeInMB() >= createNewSessionAfterDataInMB;
  }

  public Pair<BeginOfflineSessionResponse, CommandContext> beginSession(
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
            namespace,
            createCollectionCommand,
            ssTablesOutputDirectory,
            embeddingProvider,
            fileWriterBufferSizeInMB);

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
            .invoke(t -> System.out.println("Error : " + t))
            .subscribe()
            .asCompletionStage()
            .get();
    BeginOfflineSessionResponse beginOfflineSessionResponse =
        BeginOfflineSessionResponse.fromCommandResult(commandResult);
    return new ImmutablePair<>(beginOfflineSessionResponse, commandContext);
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
    // TODO - SL, what is the max number of docs in the offline insert many ?
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
    // TODO SL - response should include file path and size
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
