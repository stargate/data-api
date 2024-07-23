package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.impl.EndOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.EndOfflineSessionOperation;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class EndOfflineSessionCommandResolverTest {

  @Test
  public void verifyOperation() {
    EndOfflineSessionCommandResolver endOfflineSessionCommandResolver =
        new EndOfflineSessionCommandResolver();
    String sessionId = UUID.randomUUID().toString();
    EndOfflineSessionCommand endOfflineSessionCommand = new EndOfflineSessionCommand(sessionId);
    assertInstanceOf(
        EndOfflineSessionOperation.class,
        endOfflineSessionCommandResolver.resolveCommand(
            TestConstants.COLLECTION_CONTEXT, endOfflineSessionCommand));
    assertEquals(
        EndOfflineSessionCommand.class, endOfflineSessionCommandResolver.getCommandClass());
  }
}
