package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class OfflineGetStatusCommandTest {
  @Test
  public void testOfflineGetStatusCommand() {
    String sessionId = "sessionId";
    OfflineGetStatusCommand offlineGetStatusCommand = new OfflineGetStatusCommand(sessionId);
    assertEquals(sessionId, offlineGetStatusCommand.sessionId());
    assertThrows(
        IllegalArgumentException.class,
        () -> new OfflineGetStatusCommand(null),
        "sessionId is required");
  }
}
