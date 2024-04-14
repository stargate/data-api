package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class EndOfflineSessionCommandTest {

  @Test
  public void testSessionIdCheck() {
    String sessionId = "sessionId";
    EndOfflineSessionCommand endOfflineSessionCommand = new EndOfflineSessionCommand(sessionId);
    assertEquals(sessionId, endOfflineSessionCommand.sessionId());
    assertThrows(
        IllegalArgumentException.class,
        () -> new EndOfflineSessionCommand(null),
        "sessionId is required");
  }
}
