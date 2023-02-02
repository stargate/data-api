package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject
  DocumentConfig documentConfig;
  @Inject FindCommandResolver findCommandResolver;

  @Nested
  class FindCommandResolveCommand {

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
                {
                  "find": {
                    "filter" : {"_id" : "id"}
                  }
                }
                """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation = findCommandResolver.resolveCommand(commandContext, findCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(new FindOperation.IDFilter(FindOperation.IDFilter.Operator.EQ, "id")),
              null,
              documentConfig.maxLimit(),
              documentConfig.defaultPageSize(),
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
      String json = """
          {
            "find": {

            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(),
              null,
              documentConfig.maxLimit(),
              documentConfig.defaultPageSize(),
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
    public void noFilterConditionWithOptions() throws Exception {
      String json =
          """
              {
                "find": {
                  "options" : {
                    "limit" : 10,
                    "pageSize" : 5,
                    "pagingState" : "dlavjhvbavkjbna"
                  }
                }
              }
              """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext, List.of(), "dlavjhvbavkjbna", 10, 5, true, objectMapper);
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
            "find": {
              "filter" : {"col" : "val"}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          findCommandResolver.resolveCommand(commandContext, findOneCommand);
      FindOperation expected =
          new FindOperation(
              commandContext,
              List.of(
                  new FindOperation.TextFilter(
                      "col", FindOperation.MapFilterBase.Operator.EQ, "val")),
              null,
              documentConfig.maxLimit(),
              documentConfig.defaultPageSize(),
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
