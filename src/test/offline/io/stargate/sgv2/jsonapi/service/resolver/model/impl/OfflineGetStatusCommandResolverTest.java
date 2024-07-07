package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineGetStatusCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.OfflineGetStatusOperation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class OfflineGetStatusCommandResolverTest {

  @Test
  public void verifyOperation() {
    ObjectMapper objectMapper = new ObjectMapper();
    Shredder shredder = new Shredder(objectMapper, null, null);
    OfflineGetStatusCommandResolver offlineGetStatusCommandResolver =
        new OfflineGetStatusCommandResolver();
    String sessionId = UUID.randomUUID().toString();
    OfflineGetStatusCommand offlineGetStatusCommand = new OfflineGetStatusCommand(sessionId);
    assertInstanceOf(
        OfflineGetStatusOperation.class,
        offlineGetStatusCommandResolver.resolveCommand(null, offlineGetStatusCommand));
    assertEquals(OfflineGetStatusCommand.class, offlineGetStatusCommandResolver.getCommandClass());
  }
}
