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
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.CreateKeyspaceOperation;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
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
                assertThat(op.strategy()).isNull();
                assertThat(op.strategyOptions()).isNull();
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
                assertThat(op.strategy()).isEqualTo("SimpleStrategy");
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
                assertThat(op.strategy()).isEqualTo("SimpleStrategy");
                assertThat(op.strategyOptions()).containsEntry("replication_factor", 2);
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
                assertThat(op.strategy()).isEqualTo("NetworkTopologyStrategy");
                assertThat(op.strategyOptions())
                    .containsEntry("Boston", 2)
                    .containsEntry("Berlin", 3);
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
                assertThat(op.strategy()).isEqualTo("NetworkTopologyStrategy");
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
                  assertThat(op.strategy()).isNull();
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

    @Test
    public void rejectsInjectionInDataCenterName() throws Exception {
      // A datacenter key with characters outside the API allowlist is rejected up front.
      // The driver's SchemaBuilder.withNetworkTopologyStrategy(Map) does NOT escape map keys
      // (see OptionsUtils.extractOptionValue in java-driver-query-builder), so this allowlist
      // is the actual security control for the replication map.
      String json =
          """
            {
              "createNamespace": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "NetworkTopologyStrategy",
                        "dc1', 'class': 'SimpleStrategy" : 1
                    }
                }
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_REPLICATION_DATA_CENTER_NAME,
          "data center name in the NetworkTopologyStrategy replication options that is not supported",
          "must not be empty, more than 48 characters long, and may contain only alphanumeric, underscore, and hyphen characters",
          "The command used the unsupported data center name: 'dc1', 'class': 'SimpleStrategy'.");
    }

    @Test
    public void rejectsDataCenterNameWithSpace() throws Exception {
      String json =
          """
            {
              "createNamespace": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "NetworkTopologyStrategy",
                        "dc one" : 1
                    }
                }
              }
            }
            """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable, SchemaException.Code.UNSUPPORTED_REPLICATION_DATA_CENTER_NAME, "dc one");
    }

    @Test
    public void rejectsDataCenterNameTooLong() throws Exception {
      String dcName = RandomStringUtils.insecure().nextAlphabetic(49);
      String json =
              """
            {
              "createNamespace": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "NetworkTopologyStrategy",
                        "%s" : 1
                    }
                }
              }
            }
            """
              .formatted(dcName);

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable, SchemaException.Code.UNSUPPORTED_REPLICATION_DATA_CENTER_NAME, dcName);
    }
  }

  @Nested
  class CreateKeyspaceDataCenterNameSuccess {

    @Test
    public void hyphenatedDataCenterNameIsAllowed() throws Exception {
      // Cloud-style DC names (Astra, AWS regions) contain hyphens and must keep working.
      String json =
          """
            {
              "createNamespace": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "NetworkTopologyStrategy",
                        "us-east-1" : 3
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
                assertThat(op.strategy()).isEqualTo("NetworkTopologyStrategy");
                assertThat(op.strategyOptions()).containsEntry("us-east-1", 3);
              });
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
