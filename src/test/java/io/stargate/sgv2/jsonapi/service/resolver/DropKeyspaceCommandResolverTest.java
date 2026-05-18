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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class DropKeyspaceCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject DropNamespaceCommandResolver namespaceResolver;
  @Inject DropKeyspaceCommandResolver keyspaceResolver;

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
    Operation result = namespaceResolver.resolveCommand(commandContext, command);

    assertThat(result)
        .isInstanceOfSatisfying(
            DropKeyspaceOperation.class,
            op -> assertThat(op.name()).isEqualTo("red_star_belgrade"));
  }

  @Nested
  class DropKeyspaceNameValidation {

    @Test
    public void rejectsInjectionPayload() throws Exception {
      // Quote character would break out of the CQL identifier in DROP KEYSPACE IF EXISTS "%s".
      String json =
          """
            {
              "dropKeyspace": {
                "name" : "foo\\"; DROP KEYSPACE bar; --"
              }
            }
            """;

      DropKeyspaceCommand command = objectMapper.readValue(json, DropKeyspaceCommand.class);
      Throwable throwable =
          catchThrowable(() -> keyspaceResolver.resolveCommand(commandContext, command));

      assertThat(throwable).isInstanceOf(SchemaException.class);
      assertThat(((SchemaException) throwable).code)
          .isEqualTo(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.name());
    }

    @Test
    public void rejectsEmptyName() throws Exception {
      String json =
          """
            {
              "dropKeyspace": {
                "name" : "ok"
              }
            }
            """;

      DropKeyspaceCommand command = objectMapper.readValue(json, DropKeyspaceCommand.class);
      // Sanity: valid name still works
      assertThat(keyspaceResolver.resolveCommand(commandContext, command))
          .isInstanceOf(DropKeyspaceOperation.class);

      // Now: empty name via DropNamespace (DropKeyspaceCommand has @NotEmpty so deserialization
      // would short-circuit). DropNamespace also has @NotEmpty, so we exercise via raw record.
      var emptyCommand = new DropKeyspaceCommand("");
      Throwable throwable =
          catchThrowable(() -> keyspaceResolver.resolveCommand(commandContext, emptyCommand));

      assertThat(throwable).isInstanceOf(SchemaException.class);
      assertThat(((SchemaException) throwable).code)
          .isEqualTo(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.name());
    }

    @Test
    public void rejectsHyphenAndSpecialChars() {
      // Cassandra keyspace identifiers must be \w+; hyphens and other chars are rejected.
      var command = new DropKeyspaceCommand("foo-bar");
      Throwable throwable =
          catchThrowable(() -> keyspaceResolver.resolveCommand(commandContext, command));

      assertThat(throwable).isInstanceOf(SchemaException.class);
      assertThat(((SchemaException) throwable).code)
          .isEqualTo(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.name());
    }

    @Test
    public void deprecatedDropNamespaceRejectsInjection() {
      // Same protection applies to the deprecated dropNamespace alias.
      var command = new DropNamespaceCommand("evil\"; DROP KEYSPACE other; --");
      Throwable throwable =
          catchThrowable(() -> namespaceResolver.resolveCommand(commandContext, command));

      assertThat(throwable).isInstanceOf(SchemaException.class);
      assertThat(((SchemaException) throwable).code)
          .isEqualTo(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.name());
    }
  }
}
