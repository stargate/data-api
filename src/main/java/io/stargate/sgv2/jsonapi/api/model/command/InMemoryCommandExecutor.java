package io.stargate.sgv2.jsonapi.api.model.command;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.OFFLINE_WRITER;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineBeginWriterCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineBeginWriterCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineEndWriterCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineInsertManyCommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class InMemoryCommandExecutor {
  private final CommandProcessor commandProcessor;
  private final CommandContext commandContext;

  public InMemoryCommandExecutor(
      String namespace, String collection, String ssTablesOutputDirectory) {
    String sessionId = UUID.randomUUID().toString();
    OfflineBeginWriterCommand offlineBeginWriterCommand =
        buildOfflineBeginWriterCommand(namespace, collection, ssTablesOutputDirectory);
    FileWriterParams fileWriterParams = offlineBeginWriterCommand.getFileWriterParams();
    this.commandProcessor = buildCommandProcessor(sessionId, fileWriterParams);
    this.commandContext =
        new CommandContext(
            namespace,
            collection,
            offlineBeginWriterCommand.getCollectionSettings(),
            offlineBeginWriterCommand.getEmbeddingService());
  }

  private CommandProcessor buildCommandProcessor(
      String sessionId, FileWriterParams fileWriterParams) {
    return new CommandProcessor(
        buildQueryExecutor(sessionId, fileWriterParams), buildCommandResolverService());
  }

  protected static QueryExecutor buildQueryExecutor(
      String sessionId, FileWriterParams fileWriterParams) {
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(OperationsConfig.class)
            .withMapping(DocumentLimitsConfig.class)
            // TODO-SL increase cache expiry limit
            .withDefaultValue("stargate.jsonapi.operations.database-config.type", OFFLINE_WRITER)
            .build();
    OperationsConfig operationsConfig = smallRyeConfig.getConfigMapping(OperationsConfig.class);
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(Optional.of(sessionId), fileWriterParams);
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, new SimpleMeterRegistry());
    return new QueryExecutor(cqlSessionCache, operationsConfig);
  }

  public static OfflineBeginWriterCommand buildOfflineBeginWriterCommand(
      String namespace, String collection, String ssTablesOutputDirectory) {
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig =
        new CreateCollectionCommand.Options.VectorSearchConfig(
            3, CollectionSettings.SimilarityFunction.COSINE.toString());
    CreateCollectionCommand.Options createCollectionCommandOptions =
        new CreateCollectionCommand.Options(vectorSearchConfig, null, null);
    CreateCollectionCommand createCollectionCommand =
        new CreateCollectionCommand(collection, createCollectionCommandOptions);
    return new OfflineBeginWriterCommand(
        namespace, createCollectionCommand, ssTablesOutputDirectory);
  }

  protected static CommandResolverService buildCommandResolverService() {
    ObjectMapper objectMapper = new ObjectMapper();
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder().withMapping(DocumentLimitsConfig.class).build();
    DocumentLimitsConfig documentLimitsConfig =
        smallRyeConfig.getConfigMapping(DocumentLimitsConfig.class);
    Shredder shredder = new Shredder(objectMapper, documentLimitsConfig);
    return new CommandResolverService(
        List.of(
            new OfflineBeginWriterCommandResolver(shredder, objectMapper),
            new OfflineInsertManyCommandResolver(shredder, objectMapper),
            new OfflineEndWriterCommandResolver(shredder, objectMapper)));
  }

  public CommandResult runCommand(Command command) throws ExecutionException, InterruptedException {
    return commandProcessor
        .processCommand(commandContext, command)
        .subscribe()
        .asCompletionStage()
        .get();
  }
}
