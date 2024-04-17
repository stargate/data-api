package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.BeginOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.BeginOfflineSessionOperation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import org.junit.jupiter.api.Test;

public class BeginOfflineSessionCommandResolverTest {

  @Test
  public void verifyOperation() {
    ObjectMapper objectMapper = new ObjectMapper();
    Shredder shredder = new Shredder(objectMapper, null, null);
    BeginOfflineSessionCommandResolver beginOfflineSessionCommandResolver =
        new BeginOfflineSessionCommandResolver();
    CreateCollectionCommand createCollectionCommand =
        new CreateCollectionCommand("collection1", null);
    assertInstanceOf(
        BeginOfflineSessionOperation.class,
        beginOfflineSessionCommandResolver.resolveCommand(
            null,
            new BeginOfflineSessionCommand(
                "namespace1", createCollectionCommand, "ssTableOutputDirectory", null, 20)));
    assertEquals(
        BeginOfflineSessionCommand.class, beginOfflineSessionCommandResolver.getCommandClass());
  }
}
