package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.Mock;
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
import jakarta.inject.Inject;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject FindCommandResolver resolver;

  @Nested
  class FindCommandResolveCommand {

    @Mock CommandContext commandContext;

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
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).containsOnly(filter);
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
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.DateFilter(
                        "date_field",
                        DBFilterBase.MapFilterBase.Operator.EQ,
                        new Date(1672531200000L));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).containsOnly(filter);
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
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).containsOnly(filter);
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
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(DBFilterBase.IDFilter.Operator.IN, List.of());

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).containsOnly(filter);
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
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                DBFilterBase filter2 =
                    new DBFilterBase.TextFilter(
                        "field1", DBFilterBase.TextFilter.Operator.EQ, "value1");

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).containsOnly(filter, filter2);
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
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).isEmpty();
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
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                FindOperation.OrderBy orderBy = new FindOperation.OrderBy("username", true);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.filters()).isEmpty();
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
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                FindOperation.OrderBy orderBy = new FindOperation.OrderBy("username", false);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.filters()).isEmpty();
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
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                FindOperation.OrderBy orderBy = new FindOperation.OrderBy("username", true);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(10);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isEqualTo(5);
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.filters()).isEmpty();
              });
    }

    @Test
    public void noFilterConditionWithOptions() throws Exception {
      String json =
          """
              {
                "find": {
                  "options" : {
                    "limit" : 7,
                    "pagingState" : "dlavjhvbavkjbna"
                  }
                }
              }
              """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(7);
                assertThat(find.pagingState()).isEqualTo("dlavjhvbavkjbna");
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).isEmpty();
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
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.TextFilter(
                        "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).containsOnly(filter);
              });
    }
  }

  @Nested
  class FindCommandResolveWithProjection {

    @Mock CommandContext commandContext;

    @Test
    public void idFilterConditionAndProjection() throws Exception {
      final String json =
          """
          {
            "find": {
              "filter" : {"_id" : "id"},
              "projection": {
                "field1" : 1,
                "field2" : 1
              }
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      JsonNode projectionDef =
          objectMapper.readTree(
              """
                      {
                        "field1" : 1,
                        "field2" : 1
                      }
                      """);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection())
                    .isEqualTo(DocumentProjector.createFromDefinition(projectionDef));
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).containsOnly(filter);
              });
    }

    @Test
    public void noFilterConditionWithProjection() throws Exception {
      final String json =
          """
              {
                "find": {
                  "projection": {
                    "field1" : 1,
                    "field2" : 1
                  }
                }
              }
              """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      JsonNode projectionDef =
          objectMapper.readTree(
              """
                      {
                        "field1" : 1,
                        "field2" : 1
                      }
                      """);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection())
                    .isEqualTo(DocumentProjector.createFromDefinition(projectionDef));
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pagingState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.filters()).isEmpty();
              });
    }
  }
}
