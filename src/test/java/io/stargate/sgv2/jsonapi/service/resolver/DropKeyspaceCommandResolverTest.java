package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropKeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropNamespaceCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.DropKeyspaceOperation;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class DropKeyspaceCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject DropNamespaceCommandResolver resolver;
  @Inject DropKeyspaceCommandResolver dropKeyspaceResolver;

  private final TestConstants testConstants = new TestConstants();
  CommandContext<DatabaseSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.databaseContext();
  }

  @Test
  public void happyPath() throws Exception {
    String json =
        """
          {
            "dropNamespace": {
              "name" : "red_star_belgrade"
            }
          }
          """;

    DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
    Operation result = resolver.resolveCommand(commandContext, command);

    assertThat(result)
        .isInstanceOfSatisfying(
            DropKeyspaceOperation.class,
            op -> assertThat(op.name().asInternal()).isEqualTo("red_star_belgrade"));
  }

  @Test
  public void dropKeyspaceHappyPath() throws Exception {
    String json =
        """
          {
            "dropKeyspace": {
              "name" : "red_star_belgrade"
            }
          }
          """;

    DropKeyspaceCommand command = objectMapper.readValue(json, DropKeyspaceCommand.class);
    Operation result = dropKeyspaceResolver.resolveCommand(commandContext, command);

    assertThat(result)
        .isInstanceOfSatisfying(
            DropKeyspaceOperation.class,
            op -> assertThat(op.name().asInternal()).isEqualTo("red_star_belgrade"));
  }

  @Test
  public void dropKeyspaceAllowsLongWordName() {
    String name = "a".repeat(64);
    var command = new DropKeyspaceCommand(name);

    Operation result = dropKeyspaceResolver.resolveCommand(commandContext, command);

    assertThat(result)
        .isInstanceOfSatisfying(
            DropKeyspaceOperation.class, op -> assertThat(op.name().asInternal()).isEqualTo(name));
  }

  @Test
  public void dropKeyspaceRejectsNonWordName() {
    var command = new DropKeyspaceCommand("bad-name");

    Throwable throwable =
        catchThrowable(() -> dropKeyspaceResolver.resolveCommand(commandContext, command));

    assertThat(throwable)
        .isInstanceOf(SchemaException.class)
        .satisfies(
            e -> {
              SchemaException exception = (SchemaException) e;
              assertThat(exception.code).isEqualTo(SchemaException.Code.INVALID_KEYSPACE.name());
              assertThat(exception.getMessage()).contains("bad-name");
            });
  }
}
