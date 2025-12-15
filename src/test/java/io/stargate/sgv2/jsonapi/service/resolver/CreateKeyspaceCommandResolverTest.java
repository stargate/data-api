package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateNamespaceCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.CreateKeyspaceOperation;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateKeyspaceCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CreateNamespaceCommandResolver resolver;

  private final TestConstants testConstants = new TestConstants();

  CommandContext<DatabaseSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.databaseContext();
  }

  @Nested
  class CreateKeyspaceSuccess {

    @Test
    public void noOptions() throws Exception {
      String json =
          """
            {
              "createNamespace": {
                "name" : "red_star_belgrade"
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateKeyspaceOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("red_star_belgrade");
                assertThat(op.replicationMap())
                    .isEqualTo("{'class': 'SimpleStrategy', 'replication_factor': 1}");
              });
    }

    @Test
    public void simpleStrategy() throws Exception {
      String json =
          """
            {
              "createNamespace": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "SimpleStrategy"
                    }
                }
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateKeyspaceOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("red_star_belgrade");
                assertThat(op.replicationMap())
                    .isEqualTo("{'class': 'SimpleStrategy', 'replication_factor': 1}");
              });
    }

    @Test
    public void simpleStrategyWithReplication() throws Exception {
      String json =
          """
            {
              "createNamespace": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "SimpleStrategy",
                        "replication_factor": 2
                    }
                }
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateKeyspaceOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("red_star_belgrade");
                assertThat(op.replicationMap())
                    .isEqualTo("{'class': 'SimpleStrategy', 'replication_factor': 2}");
              });
    }

    @Test
    public void networkTopologyStrategy() throws Exception {
      String json =
          """
            {
              "createNamespace": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "NetworkTopologyStrategy",
                        "Boston": 2,
                        "Berlin": 3
                    }
                }
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateKeyspaceOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("red_star_belgrade");
                assertThat(op.replicationMap())
                    .isIn(
                        "{'class': 'NetworkTopologyStrategy', 'Boston': 2, 'Berlin': 3}",
                        "{'class': 'NetworkTopologyStrategy', 'Berlin': 3, 'Boston': 2}");
              });
    }

    @Test
    public void networkTopologyStrategyNoDataCenter() throws Exception {
      // allow, fail on the coordinator
      String json =
          """
            {
              "createNamespace": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "NetworkTopologyStrategy"
                    }
                }
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateKeyspaceOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("red_star_belgrade");
                assertThat(op.replicationMap()).isEqualTo("{'class': 'NetworkTopologyStrategy'}");
              });
    }

    @Test
    public void createKeyspaceWithSupportedName() throws Exception {
      String[] supportedName = {"a", "A", "0", "_", "a0", "0a_A", "_0a"};
      for (String name : supportedName) {
        String json =
                """
                {
                  "createNamespace": {
                      "name" : "%s"
                  }
                }
                """
                .formatted(name);

        CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
        Operation result = resolver.resolveCommand(commandContext, command);

        assertThat(result)
            .isInstanceOfSatisfying(
                CreateKeyspaceOperation.class,
                op -> {
                  assertThat(op.name()).isEqualTo(name);
                  assertThat(op.replicationMap())
                      .isEqualTo("{'class': 'SimpleStrategy', 'replication_factor': 1}");
                });
      }
    }
  }

  @Nested
  class CreateKeyspaceFailure {

    @Test
    public void createKeyspaceWithNull() throws Exception {
      String json =
          """
            {
              "createNamespace": {
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Keyspace with a name that is not supported.",
          "The supported Keyspace names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Keyspace name: '(null)'.");
    }

    @Test
    public void createKeyspaceWithEmptyName() throws Exception {
      String json =
          """
              {
                "createNamespace": {
                  "name" : ""
                }
              }
              """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Keyspace with a name that is not supported.",
          "The supported Keyspace names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Keyspace name: ''.");
    }

    @Test
    public void createKeyspaceWithBlankName() throws Exception {
      String json =
          """
                    {
                      "createNamespace": {
                         "name" : " "
                      }
                    }
                    """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Keyspace with a name that is not supported.",
          "The supported Keyspace names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Keyspace name: ' '.");
    }

    @Test
    public void createKeyspaceWithNameTooLong() throws Exception {
      String name = RandomStringUtils.insecure().nextAlphabetic(49);
      String json =
              """
            {
              "createNamespace": {
                "name": "%s"
              }
            }
            """
              .formatted(name);

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Keyspace with a name that is not supported.",
          "The supported Keyspace names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Keyspace name: '%s'.".formatted(name));
    }

    @Test
    public void createKeyspaceWithEmptySpace() throws Exception {
      String json =
          """
            {
              "createNamespace": {
                "name" : "a b"
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Keyspace with a name that is not supported.",
          "The supported Keyspace names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Keyspace name: 'a b'.");
    }

    @Test
    public void createKeyspaceWithSpecialCharacter() throws Exception {
      String json =
          """
              {
                "createNamespace": {
                  "name" : "!@-"
                }
              }
              """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Keyspace with a name that is not supported.",
          "The supported Keyspace names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Keyspace name: '!@-'.");
    }
  }

  private void verifySchemaException(
      Throwable throwable, SchemaException.Code exceptedErrorCode, String... messageSnippet) {
    assertThat(throwable)
        .isInstanceOf(SchemaException.class)
        .satisfies(
            e -> {
              SchemaException exception = (SchemaException) e;
              assertThat(exception.code).isEqualTo(exceptedErrorCode.name());
              for (String snippet : messageSnippet) {
                assertThat(exception.getMessage()).contains(snippet);
              }
            });
  }
}
