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
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.ReadAndUpdateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.NumberCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOneAndUpdateCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject DocumentShredder documentShredder;
  @Inject FindOneAndUpdateCommandResolver resolver;
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
                  "findOneAndUpdate": {
                    "filter" : {"_id" : "id"},
                    "update" : {"$set" : {"location" : "New York"}}
                  }
                }
                """;

    FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isTrue();
              assertThat(op.returnUpdatedDocument()).isFalse();
              assertThat(op.upsert()).isFalse();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(1);
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
                  "findOneAndUpdate": {
                    "filter" : {"status" : "active"},
                    "sort" : {"user" : 1},
                    "update" : {"$set" : {"location" : "New York"}}
                  }
                }
                """;

    FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isTrue();
              assertThat(op.returnUpdatedDocument()).isFalse();
              assertThat(op.upsert()).isFalse();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(1);
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
                            .isEqualTo(List.of(new FindCollectionOperation.OrderBy("user", true)));
                        assertThat(find.singleResponse()).isTrue();
                      });
            });
  }

  @Test
  public void filterConditionVectorSearch() throws Exception {
    String json =
        """
            {
              "findOneAndUpdate": {
                "filter" : {"status" : "active"},
                "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
                "update" : {"$set" : {"location" : "New York"}}
              }
            }
            """;

    FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isTrue();
              assertThat(op.returnUpdatedDocument()).isFalse();
              assertThat(op.upsert()).isFalse();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(1);
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
  public void idFilterConditionWithOptions() throws Exception {
    String json =
        """
                {
                  "findOneAndUpdate": {
                    "filter" : {"_id" : "id"},
                    "update" : {"$set" : {"location" : "New York"}},
                    "options" : {"returnDocument" : "after", "upsert": true }
                  }
                }
                """;

    FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isTrue();
              assertThat(op.returnUpdatedDocument()).isTrue();
              assertThat(op.upsert()).isTrue();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(1);
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
  public void filterConditionWithOptionsSort() throws Exception {
    String json =
        """
            {
              "findOneAndUpdate": {
                "filter" : {"age" : 35},
                "sort" : {"user.name" : 1, "user.age" : -1},
                "update" : {"$set" : {"location" : "New York"}},
                "options" : {"returnDocument" : "after", "upsert": true }
              }
            }
          """;

    FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isTrue();
              assertThat(op.returnUpdatedDocument()).isTrue();
              assertThat(op.upsert()).isTrue();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(1);
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
                        NumberCollectionFilter filter =
                            new NumberCollectionFilter(
                                "age", MapCollectionFilter.Operator.EQ, new BigDecimal(35));

                        assertThat(find.objectMapper()).isEqualTo(objectMapper);
                        assertThat(find.commandContext()).isEqualTo(commandContext);
                        assertThat(find.limit()).isEqualTo(1);
                        assertThat(find.pageSize()).isEqualTo(100);
                        assertThat(find.pageState()).isNull();
                        assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                        assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                        assertThat(find.orderBy()).hasSize(2);
                        assertThat(find.orderBy())
                            .isEqualTo(
                                List.of(
                                    new FindCollectionOperation.OrderBy("user.name", true),
                                    new FindCollectionOperation.OrderBy("user.age", false)));
                        assertThat(find.singleResponse()).isTrue();
                      });
            });
  }

  @Test
  public void dynamicFilterCondition() throws Exception {
    String json =
        """
                {
                  "findOneAndUpdate": {
                    "filter" : {"col" : "val"},
                    "update" : {"$set" : {"location" : "New York"}}
                  }
                }
                """;

    FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
    Operation operation = resolver.resolveCommand(commandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            ReadAndUpdateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext()).isEqualTo(commandContext);
              assertThat(op.returnDocumentInResponse()).isTrue();
              assertThat(op.returnUpdatedDocument()).isFalse();
              assertThat(op.upsert()).isFalse();
              assertThat(op.documentShredder()).isEqualTo(documentShredder);
              assertThat(op.updateLimit()).isEqualTo(1);
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
