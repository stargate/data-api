package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.OFFLINE_WRITER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.BeginOfflineSessionCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.EndOfflineSessionCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineGetStatusCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineInsertManyCommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class OfflineLoaderTester {
  public static void main(String[] args)
      throws ExecutionException, InterruptedException, JsonProcessingException {
    OfflineLoaderTester offlineLoaderTester = new OfflineLoaderTester();
    offlineLoaderTester.testOfflineLoader();
  }

  private void testOfflineLoader()
      throws ExecutionException, InterruptedException, JsonProcessingException {

    CommandProcessor commandProcessor =
        new CommandProcessor(buildQueryExecutor(), buildCommandResolverService());

    // Create the beginOfflineSession command so we can create a new offline session
    String namespace = "demo_namespace";
    String collection = "players";
    String ssTablesOutputDirectory = "/var/tmp/sstables_test";
    int vectorSize = 3;
    int fileWriterBufferSizeInMB = 25;
    String similarityFunction = CollectionSettings.SimilarityFunction.COSINE.toString();

    var vectorSearchConfig =
        new CreateCollectionCommand.Options.VectorSearchConfig(vectorSize, similarityFunction);
    var options = new CreateCollectionCommand.Options(null, null, null);
    var createCollectionCommand = new CreateCollectionCommand(collection, options);
    var beginOfflineSessionCommand =
        new BeginOfflineSessionCommand(
            namespace, createCollectionCommand, ssTablesOutputDirectory, fileWriterBufferSizeInMB);

    EmbeddingService embeddingService = null; // TODO
    JsonProcessingMetricsReporter jsonProcessingMetricsReporter = null; // TODO

    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo();

    CommandContext commandContext =
        CommandContext.from(
            namespace,
            collection,
            beginOfflineSessionCommand.getCollectionSettings(),
            embeddingService,
            beginOfflineSessionCommand.getClass().getSimpleName());

    CommandResult commandResult =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, beginOfflineSessionCommand)
            .subscribe()
            .asCompletionStage()
            .get();

    // TODO - SL - ? Can we make a OfflineBeginSessionResponse.fromCommandResult(commandResult) ?
    // so we can hide the CommandResult from the user and give them a typed result that understands
    // the command
    String sessionId =
        commandResult.status().get(CommandStatus.OFFLINE_WRITER_SESSION_ID).toString();
    System.out.println(commandResult);

    // TODO - SL, what is the max number of docs in the offline insert many ?
    // TODO - SL, what happens if some documents fail ?
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(
            sessionId,
            beginOfflineSessionCommand.getCollectionSettings().vectorEnabled()
                ? (List.of(
                    new ObjectMapper()
                        .readTree("{\"_id\": 1, \"name\":\"jim\",\"$vector\": [0.3,0.4,0.5]}"),
                    new ObjectMapper()
                        .readTree("{\"_id\": 2, \"name\":\"mark\",\"$vector\": [0.7,0.7,0.6]}")))
                : List.of(
                    new ObjectMapper().readTree("{\"_id\": 1, \"name\":\"jim\"}"),
                    new ObjectMapper().readTree("{\"_id\": 2, \"name\":\"mark\"}")));
    CommandResult offlineInsertManyCommandResponse =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineInsertManyCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    System.out.println(offlineInsertManyCommandResponse);

    // TODO: SL - Add a test that loops calling insertMany, checking the size of the SSTable with
    // get status and stops when it gets to 10 mb ?
    // TODO: AL - a test that is running multiple sessions at the same time

    // TODO SL - remember to include the file size
    OfflineGetStatusCommand offlineGetStatusCommand = new OfflineGetStatusCommand(sessionId);
    CommandResult offlineGetStatusCommandResponse =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineGetStatusCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    System.out.println(offlineGetStatusCommandResponse);

    // TODO SL - response should include file path and size
    EndOfflineSessionCommand offlineEndWriterCommand = new EndOfflineSessionCommand(sessionId);
    CommandResult offlineEndWriterCommandResponse =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineEndWriterCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    System.out.println(offlineEndWriterCommandResponse);

    /*offlineGetStatusCommand = new OfflineGetStatusCommand(sessionId);
    offlineGetStatusCommandResponse =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineGetStatusCommand)
            .subscribe()
            .asCompletionStage()
            .get();*/
    System.out.println(offlineGetStatusCommandResponse);
    System.out.println("Statements : ");
    System.out.println(beginOfflineSessionCommand.getFileWriterParams().createTableCQL() + ";");
    beginOfflineSessionCommand
        .getFileWriterParams()
        .indexCQLs()
        .forEach(s -> System.out.println(s + ";"));
    System.out.println(beginOfflineSessionCommand.getFileWriterParams().insertStatementCQL() + ";");
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
            new BeginOfflineSessionCommandResolver(shredder, objectMapper),
            new EndOfflineSessionCommandResolver(shredder, objectMapper),
            new OfflineGetStatusCommandResolver(shredder, objectMapper),
            new OfflineInsertManyCommandResolver(shredder, objectMapper)));
  }

  private QueryExecutor buildQueryExecutor() {
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(OperationsConfig.class)
            // TODO-SL increase cache expiry limit
            .withDefaultValue("stargate.jsonapi.operations.database-config.type", OFFLINE_WRITER)
            .build();
    OperationsConfig operationsConfig = smallRyeConfig.getConfigMapping(OperationsConfig.class);
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
    return new QueryExecutor(cqlSessionCache, operationsConfig);
  }
}
