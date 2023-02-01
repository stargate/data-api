package io.stargate.sgv3.docsapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv3.docsapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.operation.model.ReadOperation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv3.docsapi.service.shredding.Shredder;
import io.stargate.sgv3.docsapi.service.shredding.model.DocumentId;
import io.stargate.sgv3.docsapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv3.docsapi.service.updater.DocumentUpdater;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateOneResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject Shredder shredder;
  @Inject UpdateOneCommandResolver updateOneCommandResolver;

  @Nested
  class UpdateOneCommandResolveCommand {

    @Test
    public void idFilterConditionBsonType() throws Exception {
      String json =
          """
                            {
                              "updateOne": {
                                "filter" : {"_id" : "id"},
                                "update" : {"$set" : {"location" : "New York"}}
                              }
                            }
                            """;

      UpdateOneCommand updateOneCommand = objectMapper.readValue(json, UpdateOneCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          updateOneCommandResolver.resolveCommand(commandContext, updateOneCommand);
      ReadOperation readOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new FindOperation.IDFilter(
                      FindOperation.IDFilter.Operator.EQ, DocumentId.fromString("id"))),
              null,
              1,
              1,
              true,
              objectMapper);

      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("location", "New York")));
      ReadAndUpdateOperation expected =
          new ReadAndUpdateOperation(
              commandContext, readOperation, documentUpdater, false, shredder);
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
                              "updateOne": {
                                "filter" : {"col" : "val"},
                                "update" : {"$set" : {"location" : "New York"}}
                              }
                            }
                            """;

      UpdateOneCommand updateOneCommand = objectMapper.readValue(json, UpdateOneCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          updateOneCommandResolver.resolveCommand(commandContext, updateOneCommand);
      ReadOperation readOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new FindOperation.TextFilter(
                      "col", FindOperation.MapFilterBase.Operator.EQ, "val")),
              null,
              1,
              1,
              true,
              objectMapper);

      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("location", "New York")));
      ReadAndUpdateOperation expected =
          new ReadAndUpdateOperation(
              commandContext, readOperation, documentUpdater, false, shredder);
      assertThat(operation)
          .isInstanceOf(ReadAndUpdateOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }
  }
}
