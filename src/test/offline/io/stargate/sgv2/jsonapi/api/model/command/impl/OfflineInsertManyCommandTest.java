package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.junit.jupiter.api.Test;

class OfflineInsertManyCommandTest {

  @Test
  public void testOfflineInsertManyCommand() {
    String sessionId = "sessionId";
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(sessionId, null);
    assertEquals(sessionId, offlineInsertManyCommand.writerSessionId());
    assertNull(offlineInsertManyCommand.documents());
  }

  @Test
  public void testOfflineInsertManyCommandWithNullOrEmptySessionId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new OfflineInsertManyCommand(null, null),
        "writerSessionId is required");
    assertThrows(
        IllegalArgumentException.class,
        () -> new OfflineInsertManyCommand("", null),
        "writerSessionId is required");
  }

  @Test
  public void testMaxLimitDocuments() {
    String sessionId = "sessionId";
    List<JsonNode> documents = mock(List.class);
    when(documents.size()).thenReturn(1000);
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(sessionId, documents);
    assertEquals(sessionId, offlineInsertManyCommand.writerSessionId());
  }
}
