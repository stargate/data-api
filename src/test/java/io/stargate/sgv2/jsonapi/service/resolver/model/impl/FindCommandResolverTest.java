package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject DocumentConfig documentConfig;
  @Inject FindCommandResolver findCommandResolver;

  @Nested
  class FindCommandResolveCommand {

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"_id" : "id"}
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation = findCommandResolver.resolveCommand(commandContext, findCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"))),
              DocumentProjector.identityProjector(),
              null,
              documentConfig.maxLimit(),
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json = """
          {
            "find": {

            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(),
              DocumentProjector.identityProjector(),
              null,
              documentConfig.maxLimit(),
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void sort() throws Exception {
      String json =
          """
          {
            "find": {
              "sort" : ["username"]
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(),
              null,
              documentConfig.defaultPageSize(),
              documentConfig.defaultSortPageSize(),
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              0,
              documentConfig.maxSortReadLimit());
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void sortDesc() throws Exception {
      String json =
          """
        {
          "find": {
            "sort" : ["-username"]
          }
        }
        """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(),
              null,
              documentConfig.defaultPageSize(),
              documentConfig.defaultSortPageSize(),
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", false)),
              0,
              documentConfig.maxSortReadLimit());
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void noFilterConditionSortAndOptions() throws Exception {
      String json =
          """
        {
          "find": {
            "sort" : ["username"],
            "options" : {"skip" : 5, "limit" : 10}
          }
        }
        """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(),
              null,
              10,
              documentConfig.defaultSortPageSize(),
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              5,
              documentConfig.maxSortReadLimit());
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void noFilterConditionWithOptions() throws Exception {
      String json =
          """
              {
                "find": {
                  "options" : {
                    "limit" : 10,
                    "pagingState" : "dlavjhvbavkjbna"
                  }
                }
              }
              """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(),
              DocumentProjector.identityProjector(),
              "dlavjhvbavkjbna",
              10,
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"col" : "val"}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "col", DBFilterBase.MapFilterBase.Operator.EQ, "val")),
              DocumentProjector.identityProjector(),
              null,
              documentConfig.maxLimit(),
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }
  }

  @Nested
  class FindCommandResolveWithProjection {
    @Test
    public void idFilterConditionAndProjection() throws Exception {
      final JsonNode projectionDef =
          objectMapper.readTree(
              """
              {
                "field1" : 1,
                "field2" : 1
               }
               """);
      String json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "id"},
                          "projection": %s
                        }
                      }
                      """
              .formatted(projectionDef);
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation = findCommandResolver.resolveCommand(commandContext, findCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"))),
              DocumentProjector.createFromDefinition(projectionDef),
              null,
              documentConfig.maxLimit(),
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void noFilterConditionWithProjection() throws Exception {
      final JsonNode projectionDef =
          objectMapper.readTree(
              """
              {
                "field1" : 1,
                "field2" : 1
               }
               """);
      String json =
          """
                  {
                    "find": {
                      "projection" : %s
                    }
                  }
                  """
              .formatted(projectionDef);

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(),
              DocumentProjector.createFromDefinition(projectionDef),
              null,
              documentConfig.maxLimit(),
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }
  }
}
