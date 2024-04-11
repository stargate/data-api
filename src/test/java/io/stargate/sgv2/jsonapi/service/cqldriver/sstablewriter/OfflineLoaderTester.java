package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.response.BeginOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.EndOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineGetStatusResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineInsertManyResponse;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.Pair;

public class OfflineLoaderTester {
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    OfflineFileWriterInitializer.initialize();
    OfflineCommandsProcessor offlineCommandsProcessor = OfflineCommandsProcessor.getInstance();
    int iterationCount = 1;
    int threadPoolSize = 1;
    ExecutorService executeService = Executors.newFixedThreadPool(threadPoolSize);
    for (int i = 0; i < iterationCount; i++) {
      int finalI = i;
      executeService.submit(
          () -> {
            OfflineLoaderTester offlineLoaderTester = new OfflineLoaderTester();
            try {
              List<String> sessions = offlineLoaderTester.testLoadData(offlineCommandsProcessor);
              System.out.println("Thread " + finalI + " Sessions : " + sessions);
            } catch (Exception e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          });
    }
    executeService.shutdown();
    while (!executeService.isTerminated()) {
      Thread.sleep(1000);
      System.out.println("Waiting for threads to complete");
    }
    System.out.println("All threads completed");
  }

  private List<String> testLoadData(OfflineCommandsProcessor offlineCommandsProcessor)
      throws JsonProcessingException, ExecutionException, InterruptedException {
    List<String> sessionIds = new ArrayList<>();
    // System.out.println(threadId + "Started");
    int totalRecords = 10;
    int chuckSize = 10;
    int createNewSessionAfterDataInMB = 20;
    String namespace = "demo_namespace";
    String ssTablesOutputDirectory = "/var/tmp/sstables_test";
    int fileWriterBufferSizeInMB = 5;
    CreateCollectionCommand createCollectionCommand = buildTestCreateCollectionCommand();
    EmbeddingProvider embeddingProvider = null;
    boolean vectorTest =
        createCollectionCommand.options() != null
            && createCollectionCommand.options().vector() != null;
    List<JsonNode> records = getRecords(vectorTest, totalRecords);
    List<List<JsonNode>> recordsList = new ArrayList<>(Lists.partition(records, chuckSize));
    boolean newSession = true;
    String sessionId = null;
    boolean closeSession = true;
    CommandContext commandContext = null;
    for (List<JsonNode> recordList : recordsList) {
      if (newSession) {
        Pair<BeginOfflineSessionResponse, CommandContext> beginSession =
            offlineCommandsProcessor.beginSession(
                createCollectionCommand,
                namespace,
                ssTablesOutputDirectory,
                fileWriterBufferSizeInMB,
                embeddingProvider);
        BeginOfflineSessionResponse beginOfflineSessionResponse = beginSession.getLeft();
        if (beginOfflineSessionResponse.errors() != null
            && !beginOfflineSessionResponse.errors().isEmpty()) {
          throw new RuntimeException(
              "Error in begin session " + beginOfflineSessionResponse.errors());
        }
        sessionId = beginOfflineSessionResponse.sessionId();
        sessionIds.add(sessionId);
        commandContext = beginSession.getRight();
        closeSession = true;
      }
      OfflineInsertManyResponse offlineInsertManyResponse =
          offlineCommandsProcessor.loadData(sessionId, commandContext, recordList);
      if (offlineInsertManyResponse.errors() != null
          && !offlineInsertManyResponse.errors().isEmpty()) {
        throw new RuntimeException("Error in insert many " + offlineInsertManyResponse.errors());
      }
      OfflineGetStatusResponse offlineGetStatusResponse =
          offlineCommandsProcessor.getStatus(commandContext, sessionId);
      if (offlineGetStatusResponse.errors() != null
          && !offlineGetStatusResponse.errors().isEmpty()) {
        throw new RuntimeException("Error in get status " + offlineGetStatusResponse.errors());
      }
      if (OfflineCommandsProcessor.canEndSession(
          offlineGetStatusResponse.offlineWriterSessionStatus(), createNewSessionAfterDataInMB)) {
        closeSession = false;
        EndOfflineSessionResponse endOfflineSessionResponse =
            offlineCommandsProcessor.endSession(sessionId, commandContext);
        if (endOfflineSessionResponse.errors() != null
            && !endOfflineSessionResponse.errors().isEmpty()) {
          throw new RuntimeException("Error in end session " + endOfflineSessionResponse.errors());
        }
        newSession = true;
      } else {
        newSession = false;
      }
    }
    if (closeSession) {
      EndOfflineSessionResponse endOfflineSessionResponse =
          offlineCommandsProcessor.endSession(sessionId, commandContext);
      if (endOfflineSessionResponse.errors() != null
          && !endOfflineSessionResponse.errors().isEmpty()) {
        throw new RuntimeException("Error in end session " + endOfflineSessionResponse.errors());
      }
    }
    System.out.println("Thread " + Thread.currentThread().getId() + " Done!");
    return sessionIds;
  }

  private CreateCollectionCommand buildTestCreateCollectionCommand() {
    CreateCollectionCommand.Options.IdConfig idConfig =
        new CreateCollectionCommand.Options.IdConfig("uuid");
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig =
        new CreateCollectionCommand.Options.VectorSearchConfig(
            3, CollectionSettings.SimilarityFunction.COSINE.toString(), null);
    CreateCollectionCommand.Options.IndexingConfig indexingConfig = null;
    CreateCollectionCommand.Options createCollectionOptions =
        new CreateCollectionCommand.Options(idConfig, vectorSearchConfig, indexingConfig);
    return new CreateCollectionCommand("players", createCollectionOptions);
  }

  private static List<JsonNode> getRecords(boolean isVectorEnabled, int totalRecords)
      throws JsonProcessingException {
    List<JsonNode> records = new ArrayList<>();
    String template =
        """
        {"_id": %s,"name": "person%s", "rank":%s, "dob":{"$date": %s}%s}
        """;
    for (int i = 0; i < totalRecords; i++) {
      records.add(
          new ObjectMapper()
              .readTree(
                  String.format(
                      template,
                      i,
                      i,
                      (int) (Math.random() * 99) + 1,
                      Instant.ofEpochMilli(System.currentTimeMillis()).toEpochMilli(),
                      isVectorEnabled ? ",\"$vector\": [0.3,0.4,0.5]" : "")));
    }
    return records;
  }
}
