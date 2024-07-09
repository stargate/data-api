package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.CreateNamespaceOperation;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateNamespaceCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CreateNamespaceCommandResolver resolver;

  CommandContext<CollectionSchemaObject> commandContext = TestConstants.COLLECTION_CONTEXT;

  @Nested
  class ResolveCommand {

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
              CreateNamespaceOperation.class,
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
              CreateNamespaceOperation.class,
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
              CreateNamespaceOperation.class,
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
              CreateNamespaceOperation.class,
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
              CreateNamespaceOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("red_star_belgrade");
                assertThat(op.replicationMap()).isEqualTo("{'class': 'NetworkTopologyStrategy'}");
              });
    }
  }
}
