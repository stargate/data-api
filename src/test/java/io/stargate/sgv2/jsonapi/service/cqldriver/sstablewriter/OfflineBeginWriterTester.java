package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.OFFLINE_WRITER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineBeginWriterCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.CommandLevelLoggingConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.InsertManyCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineBeginWriterCommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class OfflineBeginWriterTester {
  public static void main(String[] args)
      throws JsonProcessingException, ExecutionException, InterruptedException {
    String namespace = "demo_namespace";
    String collection = "players";
    CreateCollectionCommand createCollectionCommand =
        new CreateCollectionCommand(
            collection,
            new CreateCollectionCommand.Options(
                new CreateCollectionCommand.Options.VectorSearchConfig(
                    3, CollectionSettings.SimilarityFunction.COSINE.toString()),
                null,
                null));
    OfflineBeginWriterCommand offlineBeginWriterCommand =
        new OfflineBeginWriterCommand(createCollectionCommand, "/var/tmp/sstables_test");
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(OperationsConfig.class)
            .withMapping(JsonApiMetricsConfig.class)
            .withMapping(MetricsConfig.class)
            .withMapping(CommandLevelLoggingConfig.class)
            .withMapping(DocumentLimitsConfig.class)
            // TODO-SL increase cache expiry limit
            .withDefaultValue("stargate.jsonapi.operations.database-config.type", OFFLINE_WRITER)
            .build();
    DocumentLimitsConfig documentLimitsConfig =
        smallRyeConfig.getConfigMapping(DocumentLimitsConfig.class);
    OperationsConfig operationsConfig = smallRyeConfig.getConfigMapping(OperationsConfig.class);
    CommandResolverService commandResolverService =
        buildCommandResolverService(documentLimitsConfig);
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo(null, null);
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, new SimpleMeterRegistry());
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
    CommandProcessor commandProcessor = new CommandProcessor(queryExecutor, commandResolverService);
    CommandResult commandResult =
        commandProcessor
            .processCommand(new CommandContext(namespace, collection), offlineBeginWriterCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    System.out.println(commandResult);
  }

  private static CommandResolverService buildCommandResolverService(
      DocumentLimitsConfig documentLimitsConfig) {
    ObjectMapper objectMapper = new ObjectMapper();
    Shredder shredder = new Shredder(objectMapper, documentLimitsConfig);
    return new CommandResolverService(
        List.of(new OfflineBeginWriterCommandResolver(shredder, objectMapper)));
  }
}
