package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineInsertManyCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class OfflineInsertManyCommandResolverTest {

  @Test
  public void verifyOperation() {
    ObjectMapper objectMapper = new ObjectMapper();
    DocumentShredder documentShredder = new DocumentShredder(objectMapper, null, null);
    OperationsConfig operationsConfig = mock(OperationsConfig.class);
    OperationsConfig.OfflineModeConfig offlineModeConfig =
        mock(OperationsConfig.OfflineModeConfig.class);
    when(offlineModeConfig.maxDocumentInsertCount()).thenReturn(1000);
    when(operationsConfig.offlineModeConfig()).thenReturn(offlineModeConfig);
    OfflineInsertManyCommandResolver offlineInsertManyCommandResolver =
        new OfflineInsertManyCommandResolver(documentShredder, operationsConfig);
    String sessionId = UUID.randomUUID().toString();
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(sessionId, List.of());
    assertInstanceOf(
        InsertCollectionOperation.class,
        offlineInsertManyCommandResolver.resolveCommand(
            TestConstants.COLLECTION_CONTEXT, offlineInsertManyCommand));
    assertEquals(
        OfflineInsertManyCommand.class, offlineInsertManyCommandResolver.getCommandClass());
  }

  @Test
  public void verifyOperationWhenMaxDocsExceeded() {
    ObjectMapper objectMapper = new ObjectMapper();
    DocumentShredder documentShredder = new DocumentShredder(objectMapper, null, null);
    OperationsConfig operationsConfig = mock(OperationsConfig.class);
    OperationsConfig.OfflineModeConfig offlineModeConfig =
        mock(OperationsConfig.OfflineModeConfig.class);
    when(offlineModeConfig.maxDocumentInsertCount()).thenReturn(1000);
    when(operationsConfig.offlineModeConfig()).thenReturn(offlineModeConfig);
    OfflineInsertManyCommandResolver offlineInsertManyCommandResolver =
        new OfflineInsertManyCommandResolver(documentShredder, operationsConfig);
    String sessionId = UUID.randomUUID().toString();
    List<JsonNode> docs = mock(List.class);
    when(docs.size()).thenReturn(1001);
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(sessionId, docs);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            offlineInsertManyCommandResolver.resolveCommand(
                TestConstants.COLLECTION_CONTEXT, offlineInsertManyCommand),
        "Exceeded max document insert count");
  }
}
