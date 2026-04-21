package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.ReadAndUpdateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateManyCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject DocumentShredder documentShredder;
  @Inject UpdateManyCommandResolver resolver;
  @InjectMock protected RequestContext dataApiRequestInfo;
  private final TestConstants testConstants = new TestConstants();

  CommandContext<CollectionSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.collectionContext();
  }

  @Test
  public void idFilterCondition() throws Exception {
    String json =
        """
          {
            "updateMany": {
              "filter" : {"_id" : "id"},
              "update" : {"$set" : {"location" : "New York"}}
            }
          }
          """;

    UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isFalse();
              assertThat(op.returnUpdatedDocument()).isFalse();
              assertThat(op.upsert()).isFalse();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(operationsConfig.maxDocumentUpdateCount());
              assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
              assertThat(op.documentUpdater())
                  .isInstanceOfSatisfying(
                      DocumentUpdater.class,
                      updater -> {
                        UpdateClause updateClause =
                            DocumentUpdaterUtils.updateClause(
                                UpdateOperator.SET,
                                objectMapper.createObjectNode().put("location", "New York"));

                        assertThat(updater.updateOperations())
                            .isEqualTo(updateClause.buildOperations());
                      });
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
                        assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                        assertThat(find.pageState()).isNull();
                        assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                        assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                      });
            });
  }

  @Test
  public void noFilterCondition() throws Exception {
    String json =
        """
          {
            "updateMany": {
              "update" : {"$set" : {"location" : "New York"}}
            }
          }
          """;

    UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isFalse();
              assertThat(op.returnUpdatedDocument()).isFalse();
              assertThat(op.upsert()).isFalse();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(operationsConfig.maxDocumentUpdateCount());
              assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
              assertThat(op.documentUpdater())
                  .isInstanceOfSatisfying(
                      DocumentUpdater.class,
                      updater -> {
                        UpdateClause updateClause =
                            DocumentUpdaterUtils.updateClause(
                                UpdateOperator.SET,
                                objectMapper.createObjectNode().put("location", "New York"));

                        assertThat(updater.updateOperations())
                            .isEqualTo(updateClause.buildOperations());
                      });
              assertThat(op.findCollectionOperation())
                  .isInstanceOfSatisfying(
                      FindCollectionOperation.class,
                      find -> {
                        assertThat(find.objectMapper()).isEqualTo(objectMapper);
                        assertThat(find.commandContext()).isEqualTo(commandContext);
                        assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                        assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                        assertThat(find.pageState()).isNull();
                        assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                        assertThat(find.dbLogicalExpression().filters()).isEmpty();
                      });
            });
  }

  @Test
  public void dynamicFilterCondition() throws Exception {
    String json =
        """
          {
            "updateMany": {
              "filter" : {"col" : "val"},
                "update" : {"$set" : {"location" : "New York"}}
              }
            }
          """;

    UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isFalse();
              assertThat(op.returnUpdatedDocument()).isFalse();
              assertThat(op.upsert()).isFalse();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(operationsConfig.maxDocumentUpdateCount());
              assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
              assertThat(op.documentUpdater())
                  .isInstanceOfSatisfying(
                      DocumentUpdater.class,
                      updater -> {
                        UpdateClause updateClause =
                            DocumentUpdaterUtils.updateClause(
                                UpdateOperator.SET,
                                objectMapper.createObjectNode().put("location", "New York"));

                        assertThat(updater.updateOperations())
                            .isEqualTo(updateClause.buildOperations());
                      });
              assertThat(op.findCollectionOperation())
                  .isInstanceOfSatisfying(
                      FindCollectionOperation.class,
                      find -> {
                        TextCollectionFilter filter =
                            new TextCollectionFilter("col", MapCollectionFilter.Operator.EQ, "val");

                        assertThat(find.objectMapper()).isEqualTo(objectMapper);
                        assertThat(find.commandContext()).isEqualTo(commandContext);
                        assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                        assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                        assertThat(find.pageState()).isNull();
                        assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                        assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                      });
            });
  }

  @Test
  public void withUpsert() throws Exception {
    String json =
        """
          {
            "updateMany": {
              "update" : {"$set" : {"location" : "New York"}},
              "options": { "upsert": true }
            }
          }
          """;

    UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isFalse();
              assertThat(op.returnUpdatedDocument()).isFalse();
              assertThat(op.upsert()).isTrue();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(operationsConfig.maxDocumentUpdateCount());
              assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
              assertThat(op.documentUpdater())
                  .isInstanceOfSatisfying(
                      DocumentUpdater.class,
                      updater -> {
                        UpdateClause updateClause =
                            DocumentUpdaterUtils.updateClause(
                                UpdateOperator.SET,
                                objectMapper.createObjectNode().put("location", "New York"));

                        assertThat(updater.updateOperations())
                            .isEqualTo(updateClause.buildOperations());
                      });
              assertThat(op.findCollectionOperation())
                  .isInstanceOfSatisfying(
                      FindCollectionOperation.class,
                      find -> {
                        assertThat(find.objectMapper()).isEqualTo(objectMapper);
                        assertThat(find.commandContext()).isEqualTo(commandContext);
                        assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                        assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                        assertThat(find.pageState()).isNull();
                        assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                        assertThat(find.dbLogicalExpression().filters()).isEmpty();
                      });
            });
  }
}
