package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOneCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject FindOneCommandResolver findOneCommandResolver;

  @Nested
  class FindOneCommandResolveCommand {

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
                {
                  "findOne": {
                    "sort": [
                      "user.name",
                      "-user.age"
                    ],
                    "filter" : {"_id" : "id"}
                  }
                }
                """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findOneCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"))),
              null,
              1,
              1,
              true,
              objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
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
            "findOne": {
              "sort": [
                "user.name",
                "-user.age"
              ]
            }
          }
          """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findOneCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(commandContext, List.of(), null, 1, 1, true, objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
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
            "findOne": {
              "sort": [
                "user.name",
                "-user.age"
              ],
              "filter" : {"col" : "val"}
            }
          }
          """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      final CommandContext commandContext = new CommandContext("namespace", "collection");
      final Operation operation =
          findOneCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "col", DBFilterBase.MapFilterBase.Operator.EQ, "val")),
              null,
              1,
              1,
              true,
              objectMapper);
      assertThat(operation)
          .isInstanceOf(FindOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }
  }
}
