package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.Mock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.TruncateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteManyCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject DeleteManyCommandResolver resolver;

  @Nested
  class ResolveCommand {

    @Mock CommandContext commandContext;

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteMany": {
              "filter" : {"_id" : "id"}
            }
          }
          """;

      DeleteManyCommand deleteManyCommand = objectMapper.readValue(json, DeleteManyCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteManyCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(operationsConfig.maxDocumentDeleteCount());
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
                          assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                          assertThat(find.limit())
                              .isEqualTo(operationsConfig.maxDocumentDeleteCount() + 1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.KEY);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                        });
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteMany": {
            }
          }
          """;

      DeleteManyCommand deleteManyCommand = objectMapper.readValue(json, DeleteManyCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteManyCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              TruncateCollectionOperation.class,
              op -> {
                assertThat(op.context()).isEqualTo(commandContext);
              });
    }

    @Test
    public void emptyFilterCondition() throws Exception {
      String json =
          """
                {
                  "deleteMany": {
                    "filter" : {}
                  }
                }
                """;

      DeleteManyCommand deleteManyCommand = objectMapper.readValue(json, DeleteManyCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteManyCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              TruncateCollectionOperation.class,
              op -> {
                assertThat(op.context()).isEqualTo(commandContext);
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteMany": {
              "filter" : {"col" : "val"}
            }
          }
          """;

      DeleteManyCommand deleteManyCommand = objectMapper.readValue(json, DeleteManyCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteManyCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(operationsConfig.maxDocumentDeleteCount());
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
                          assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                          assertThat(find.limit())
                              .isEqualTo(operationsConfig.maxDocumentDeleteCount() + 1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.KEY);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                        });
              });
    }
  }
}
