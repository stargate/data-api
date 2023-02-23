package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteManyCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject DocumentConfig documentConfig;
  @Inject DeleteManyCommandResolver deleteManyCommandResolver;

  @Nested
  class DeleteManyCommandResolveCommand {

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
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          deleteManyCommandResolver.resolveCommand(commandContext, deleteManyCommand);
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"))),
              null,
              documentConfig.maxLimit(),
              documentConfig.maxDocumentDeleteCount(),
              ReadType.KEY,
              objectMapper);
      DeleteOperation expected = new DeleteOperation(commandContext, findOperation);
      assertThat(operation)
          .isInstanceOf(DeleteOperation.class)
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
                      "deleteMany": {
                      }
                    }
                    """;

      DeleteManyCommand deleteManyCommand = objectMapper.readValue(json, DeleteManyCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          deleteManyCommandResolver.resolveCommand(commandContext, deleteManyCommand);
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(),
              null,
              documentConfig.maxLimit(),
              documentConfig.maxDocumentDeleteCount(),
              ReadType.KEY,
              objectMapper);
      DeleteOperation expected = new DeleteOperation(commandContext, findOperation);
      assertThat(operation)
          .isInstanceOf(DeleteOperation.class)
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
                      "deleteMany": {
                        "filter" : {"col" : "val"}
                      }
                    }
                    """;

      DeleteManyCommand deleteManyCommand = objectMapper.readValue(json, DeleteManyCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          deleteManyCommandResolver.resolveCommand(commandContext, deleteManyCommand);
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "col", DBFilterBase.MapFilterBase.Operator.EQ, "val")),
              null,
              documentConfig.maxLimit(),
              documentConfig.maxDocumentDeleteCount(),
              ReadType.KEY,
              objectMapper);
      DeleteOperation expected = new DeleteOperation(commandContext, findOperation);
      assertThat(operation)
          .isInstanceOf(DeleteOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }
  }
}
