package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.Date;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
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
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void dateFilterCondition() throws Exception {
      String json =
          """
              {
                "find": {
                  "filter" : {"date_field" : {"$date" : 1672531200000}}
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
                  new DBFilterBase.DateFilter(
                      "date_field",
                      DBFilterBase.MapFilterBase.Operator.EQ,
                      new Date(1672531200000L))),
              DocumentProjector.identityProjector(),
              null,
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void idFilterWithInOperatorCondition() throws Exception {
      String json =
          """
              {
                "find": {
                  "filter" : {"_id" : { "$in" : ["id1", "id2"]}}
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
                      DBFilterBase.IDFilter.Operator.IN,
                      List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")))),
              DocumentProjector.identityProjector(),
              null,
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void idFilterWithInOperatorEmptyArrayCondition() throws Exception {
      String json =
          """
                  {
                    "find": {
                      "filter" : {"_id" : { "$in" : []}}
                    }
                  }
                  """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation = findCommandResolver.resolveCommand(commandContext, findCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(new DBFilterBase.IDFilter(DBFilterBase.IDFilter.Operator.IN, List.of())),
              DocumentProjector.identityProjector(),
              null,
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }

    @Test
    public void byIdInAndOtherConditionTogether() throws Exception {
      String json =
          """
        {
          "find": {
            "filter" : {"_id" : { "$in" : ["id1", "id2"]}, "field1" : "value1" }
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
                      DBFilterBase.IDFilter.Operator.IN,
                      List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2"))),
                  new DBFilterBase.TextFilter(
                      "field1", DBFilterBase.TextFilter.Operator.EQ, "value1")),
              DocumentProjector.identityProjector(),
              null,
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
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
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
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
              "sort" : {"username" : 1}
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
              operationsConfig.defaultPageSize(),
              operationsConfig.defaultSortPageSize(),
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              0,
              operationsConfig.maxDocumentSortCount());
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
            "sort" : {"username" : -1}
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
              operationsConfig.defaultPageSize(),
              operationsConfig.defaultSortPageSize(),
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", false)),
              0,
              operationsConfig.maxDocumentSortCount());
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
            "sort" : {"username" : 1},
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
              DocumentProjector.identityProjector(),
              null,
              10,
              operationsConfig.defaultSortPageSize(),
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              5,
              operationsConfig.maxDocumentSortCount());
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
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
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
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
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
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
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
              Integer.MAX_VALUE,
              operationsConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }
  }
}
