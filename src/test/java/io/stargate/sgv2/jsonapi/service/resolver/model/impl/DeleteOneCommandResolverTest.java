package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingService;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteOneCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject DeleteOneCommandResolver resolver;

  @Nested
  class DeleteOneCommandResolveCommand {

    CommandContext commandContext = CommandContext.empty();

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteOne": {
              "filter" : {"_id" : "id"}
            }
          }
          """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.IDFilter filter =
                              new DBFilterBase.IDFilter(
                                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pagingState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.KEY);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteOne": {
            }
          }
          """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pagingState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.KEY);
                          assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteOne": {
              "filter" : {"col" : "val"}
            }
          }
          """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pagingState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.KEY);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterConditionWithSort() throws Exception {
      String json =
          """
                  {
                    "deleteOne": {
                      "filter" : {"col" : "val"},
                      "sort" : {"sort_col" : 1}
                    }
                  }
                  """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pagingState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(1);
                          assertThat(find.orderBy().get(0))
                              .isEqualTo(new FindOperation.OrderBy("sort_col", true));
                          assertThat(find.maxSortReadLimit())
                              .isEqualTo(operationsConfig.maxDocumentSortCount());
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterConditionWithVectorSearch() throws Exception {
      String json =
          """
              {
                "deleteOne": {
                  "filter" : {"col" : "val"},
                  "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]}
                }
              }
              """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pagingState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.KEY);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.orderBy()).isNull();
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.11f, 0.22f, 0.33f, 0.44f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterConditionWithVectorizeSearch() throws Exception {
      String json =
          """
              {
                "deleteOne": {
                  "filter" : {"col" : "val"},
                  "sort" : {"$vectorize" : "test data"}
                }
              }
              """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation =
          resolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pagingState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.KEY);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.orderBy()).isNull();
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.25f, 0.25f, 0.25f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }
  }
}
