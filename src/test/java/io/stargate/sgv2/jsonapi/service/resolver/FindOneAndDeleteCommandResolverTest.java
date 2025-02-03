package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOneAndDeleteCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject FindOneAndDeleteCommandResolver resolver;
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class Resolve {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.collectionContext();

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
        {
          "findOneAndDelete": {
            "filter" : {"_id" : "id"}
          }
        }
        """;
      FindOneAndDeleteCommand command = objectMapper.readValue(json, FindOneAndDeleteCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
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
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void filterConditionSort() throws Exception {
      String json =
          """
        {
          "findOneAndDelete": {
            "filter" : {"status" : "active"},
            "sort" : {"user" : 1}
          }
        }
        """;

      FindOneAndDeleteCommand command = objectMapper.readValue(json, FindOneAndDeleteCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "status", MapCollectionFilter.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(1);
                          assertThat(find.orderBy())
                              .isEqualTo(
                                  List.of(new FindCollectionOperation.OrderBy("user", true)));
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void filterConditionVectorSearch() throws Exception {
      String json =
          """
                {
                  "findOneAndDelete": {
                    "filter" : {"status" : "active"},
                    "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]}
                  }
                }
                """;

      FindOneAndDeleteCommand command = objectMapper.readValue(json, FindOneAndDeleteCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "status", MapCollectionFilter.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.11f, 0.22f, 0.33f, 0.44f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void idFilterWithProjection() throws Exception {
      String json =
          """
          {
            "findOneAndDelete": {
              "filter" : {"_id" : "id"},
              "projection" : { "x":0, "subdoc.c":0 }
            }
          }
          """;
      FindOneAndDeleteCommand command = objectMapper.readValue(json, FindOneAndDeleteCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      JsonNode projection = objectMapper.readTree("{\"x\":0, \"subdoc.c\":0}");
      final DocumentProjector projector = DocumentProjector.createFromDefinition(projection);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.resultProjection()).isEqualTo(projector);
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          IDCollectionFilter filter =
                              new IDCollectionFilter(
                                  IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }
  }
}
