package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineInsertManyCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class OfflineInsertManyCommandResolverTest {

  @Test
  public void verifyOperation() {
    ObjectMapper objectMapper = new ObjectMapper();
    Shredder shredder = new Shredder(objectMapper, null, null);
    OfflineInsertManyCommandResolver offlineInsertManyCommandResolver =
        new OfflineInsertManyCommandResolver(shredder);
    String sessionId = UUID.randomUUID().toString();
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(sessionId, List.of());
    assertInstanceOf(
        InsertOperation.class,
        offlineInsertManyCommandResolver.resolveCommand(null, offlineInsertManyCommand));
    assertEquals(
        OfflineInsertManyCommand.class, offlineInsertManyCommandResolver.getCommandClass());
  }
}
