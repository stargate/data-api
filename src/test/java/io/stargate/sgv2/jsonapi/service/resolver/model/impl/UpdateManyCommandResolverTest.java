package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
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
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateManyCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject DocumentConfig documentConfig;
  @Inject Shredder shredder;

  @Inject UpdateManyCommandResolver updateManyCommandResolver;

  @Nested
  class UpdateManyCommandResolveCommand {

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

      UpdateManyCommand updateManyCommand = objectMapper.readValue(json, UpdateManyCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          updateManyCommandResolver.resolveCommand(commandContext, updateManyCommand);
      FindOperation readOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"))),
              null,
              documentConfig.maxDocumentUpdateCount() + 1,
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("location", "New York")));
      ReadAndUpdateOperation expected =
          new ReadAndUpdateOperation(
              commandContext,
              readOperation,
              documentUpdater,
              false,
              false,
              false,
              shredder,
              documentConfig.maxDocumentUpdateCount(),
              3);

      assertThat(operation)
          .isInstanceOf(ReadAndUpdateOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
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

      UpdateManyCommand updateManyCommand = objectMapper.readValue(json, UpdateManyCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          updateManyCommandResolver.resolveCommand(commandContext, updateManyCommand);
      FindOperation readOperation =
          new FindOperation(
              commandContext,
              List.of(),
              null,
              documentConfig.maxDocumentUpdateCount() + 1,
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("location", "New York")));
      ReadAndUpdateOperation expected =
          new ReadAndUpdateOperation(
              commandContext,
              readOperation,
              documentUpdater,
              false,
              false,
              false,
              shredder,
              documentConfig.maxDocumentUpdateCount(),
              3);

      assertThat(operation)
          .isInstanceOf(ReadAndUpdateOperation.class)
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
                                      "updateMany": {
                                      "filter" : {"col" : "val"},
                                        "update" : {"$set" : {"location" : "New York"}}
                                      }
                                    }
                              """;

      UpdateManyCommand updateManyCommand = objectMapper.readValue(json, UpdateManyCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          updateManyCommandResolver.resolveCommand(commandContext, updateManyCommand);
      FindOperation readOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "col", DBFilterBase.MapFilterBase.Operator.EQ, "val")),
              null,
              documentConfig.maxDocumentUpdateCount() + 1,
              documentConfig.defaultPageSize(),
              ReadType.DOCUMENT,
              objectMapper);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("location", "New York")));

      ReadAndUpdateOperation expected =
          new ReadAndUpdateOperation(
              commandContext,
              readOperation,
              documentUpdater,
              false,
              false,
              false,
              shredder,
              documentConfig.maxDocumentUpdateCount(),
              3);
      assertThat(operation)
          .isInstanceOf(ReadAndUpdateOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }
  }
}
