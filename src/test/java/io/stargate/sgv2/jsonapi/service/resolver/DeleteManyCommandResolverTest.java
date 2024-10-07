package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.TruncateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
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
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class ResolveCommand {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.COLLECTION_CONTEXT;

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
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(operationsConfig.maxDocumentDeleteCount());
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          IDCollectionFilter filter =
                              new IDCollectionFilter(
                                  IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                          assertThat(find.limit())
                              .isEqualTo(operationsConfig.maxDocumentDeleteCount() + 1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.KEY);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(operationsConfig.maxDocumentDeleteCount());
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "col", MapCollectionFilter.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                          assertThat(find.limit())
                              .isEqualTo(operationsConfig.maxDocumentDeleteCount() + 1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.KEY);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                        });
              });
    }
  }
}
