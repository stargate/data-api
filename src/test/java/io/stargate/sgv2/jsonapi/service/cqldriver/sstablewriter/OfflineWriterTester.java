package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.InMemoryCommandExecutor;
import io.stargate.sgv2.jsonapi.api.model.command.impl.BeginOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.EndOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineGetStatusCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineInsertManyCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class OfflineWriterTester {
  public static void main(String[] args)
      throws ExecutionException, InterruptedException, JsonProcessingException {
    OfflineWriterTester offlineWriterTester = new OfflineWriterTester();
    offlineWriterTester.testOfflineWriter();
  }

  private void testOfflineWriter()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    String namespace = "demo_namespace";
    String collection = "players";
    String ssTablesOutputDirectory = "/var/tmp/sstables_test";

    InMemoryCommandExecutor inMemoryCommandExecutor =
        new InMemoryCommandExecutor(
            namespace,
            collection,
            ssTablesOutputDirectory,
            true,
            3,
            CollectionSettings.SimilarityFunction.COSINE.toString());

    BeginOfflineSessionCommand offlineBeginWriterCommand =
        InMemoryCommandExecutor.buildOfflineBeginWriterCommand(
            namespace,
            collection,
            ssTablesOutputDirectory,
            true,
            3,
            CollectionSettings.SimilarityFunction.COSINE.toString());
    CommandResult offlineBeingWirterCommandResponse =
        inMemoryCommandExecutor.runCommand(offlineBeginWriterCommand);
    System.out.println("begin command: " + offlineBeingWirterCommandResponse);
    String sessionId =
        offlineBeingWirterCommandResponse
            .status()
            .get(CommandStatus.OFFLINE_WRITER_SESSION_ID)
            .toString();

    List<JsonNode> documents =
        List.of(
            new ObjectMapper()
                .readTree("{\"_id\": 1, \"name\":\"jim\",\"$vector\": [0.3,0.4,0.5]}"),
            new ObjectMapper()
                .readTree("{\"_id\": 2, \"name\":\"mark\",\"$vector\": [0.7,0.7,0.6]}"));
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(sessionId, documents);
    CommandResult offlineInsertManyCommandResponse =
        inMemoryCommandExecutor.runCommand(offlineInsertManyCommand);
    System.out.println("insert many response: " + offlineInsertManyCommandResponse);

    OfflineGetStatusCommand offlineGetStatusCommand = new OfflineGetStatusCommand(sessionId);
    CommandResult offlineGetStatusCommandResponse =
        inMemoryCommandExecutor.runCommand(offlineGetStatusCommand);
    System.out.println("status:" + offlineGetStatusCommandResponse);

    EndOfflineSessionCommand offlineEndWriterCommand = new EndOfflineSessionCommand(sessionId);
    CommandResult offlineEndWriterCommandResponse =
        inMemoryCommandExecutor.runCommand(offlineEndWriterCommand);
    System.out.println("end command: " + offlineEndWriterCommandResponse);

    offlineGetStatusCommand = new OfflineGetStatusCommand(sessionId);
    offlineGetStatusCommandResponse = inMemoryCommandExecutor.runCommand(offlineGetStatusCommand);
    System.out.println("status again:" + offlineGetStatusCommandResponse);
  }
}
