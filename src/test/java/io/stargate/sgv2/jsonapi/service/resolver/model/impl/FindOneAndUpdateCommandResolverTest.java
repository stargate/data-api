package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.Mock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import java.math.BigDecimal;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOneAndUpdateCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject DocumentConfig documentConfig;
  @Inject Shredder shredder;
  @Inject FindOneAndUpdateCommandResolver resolver;

  @Nested
  class Resolve {

    @Mock CommandContext commandContext;

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
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(documentConfig.lwt().retries());
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
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(find.filters()).singleElement().isEqualTo(filter);
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
              "sort" : ["user"],
              "update" : {"$set" : {"location" : "New York"}}
            }
          }
          """;

      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(documentConfig.lwt().retries());
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
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pagingState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                          assertThat(find.filters()).singleElement().isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(1);
                          assertThat(find.orderBy())
                              .isEqualTo(List.of(new FindOperation.OrderBy("user", true)));
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
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isTrue();
                assertThat(op.upsert()).isTrue();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(documentConfig.lwt().retries());
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
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(find.filters()).singleElement().isEqualTo(filter);
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
                  "sort": [
                    "user.name",
                    "-user.age"
                  ],
                  "update" : {"$set" : {"location" : "New York"}},
                  "options" : {"returnDocument" : "after", "upsert": true }
                }
              }
              """;

      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isTrue();
                assertThat(op.upsert()).isTrue();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(documentConfig.lwt().retries());
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
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.NumberFilter filter =
                              new DBFilterBase.NumberFilter(
                                  "age",
                                  DBFilterBase.MapFilterBase.Operator.EQ,
                                  new BigDecimal(35));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.pagingState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                          assertThat(find.filters()).singleElement().isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(2);
                          assertThat(find.orderBy())
                              .isEqualTo(
                                  List.of(
                                      new FindOperation.OrderBy("user.name", true),
                                      new FindOperation.OrderBy("user.age", false)));
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
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(documentConfig.lwt().retries());
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
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(find.filters()).singleElement().isEqualTo(filter);
                        });
              });
    }
  }
}
