package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.InMemoryCommandExecutor;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineBeginWriterCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineEndWriterCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineInsertManyCommand;
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
        new InMemoryCommandExecutor(namespace, collection, ssTablesOutputDirectory);

    OfflineBeginWriterCommand offlineBeginWriterCommand =
        InMemoryCommandExecutor.buildOfflineBeginWriterCommand(
            namespace, collection, ssTablesOutputDirectory);
    CommandResult offlineBeingWirterCommandResponse =
        inMemoryCommandExecutor.runCommand(offlineBeginWriterCommand);
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
    System.out.println(offlineInsertManyCommandResponse);

    OfflineEndWriterCommand offlineEndWriterCommand = new OfflineEndWriterCommand(sessionId);
    CommandResult offlineEndWriterCommandResponse =
        inMemoryCommandExecutor.runCommand(offlineEndWriterCommand);
    System.out.println(offlineEndWriterCommandResponse);
  }
}
