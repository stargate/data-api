package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateDatabaseCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateDatabaseOperation;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateDatabaseResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CreateDatabaseResolver resolver;

  @Nested
  class ResolveCommand {

    @Test
    public void noOptions() throws Exception {
      String json =
          """
            {
              "createDatabase": {
                "name" : "red_star_belgrade"
              }
            }
            """;

      CreateDatabaseCommand command = objectMapper.readValue(json, CreateDatabaseCommand.class);
      Operation result = resolver.resolveCommand(null, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateDatabaseOperation.class,
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
              "createDatabase": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "SimpleStrategy"
                    }
                }
              }
            }
            """;

      CreateDatabaseCommand command = objectMapper.readValue(json, CreateDatabaseCommand.class);
      Operation result = resolver.resolveCommand(null, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateDatabaseOperation.class,
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
              "createDatabase": {
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

      CreateDatabaseCommand command = objectMapper.readValue(json, CreateDatabaseCommand.class);
      Operation result = resolver.resolveCommand(null, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateDatabaseOperation.class,
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
              "createDatabase": {
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

      CreateDatabaseCommand command = objectMapper.readValue(json, CreateDatabaseCommand.class);
      Operation result = resolver.resolveCommand(null, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateDatabaseOperation.class,
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
              "createDatabase": {
                "name" : "red_star_belgrade",
                "options": {
                    "replication": {
                        "class": "NetworkTopologyStrategy"
                    }
                }
              }
            }
            """;

      CreateDatabaseCommand command = objectMapper.readValue(json, CreateDatabaseCommand.class);
      Operation result = resolver.resolveCommand(null, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateDatabaseOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("red_star_belgrade");
                assertThat(op.replicationMap()).isEqualTo("{'class': 'NetworkTopologyStrategy'}");
              });
    }
  }
}
