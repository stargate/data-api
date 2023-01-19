package io.stargate.sgv3.docsapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.FindOperation;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteOneCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject DeleteOneCommandResolver deleteOneCommandResolver;

  @Nested
  class DeleteOneCommandResolveCommand {

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
                          {
                            "deleteOne": {
                              "filter" : {"_id" : "id"}
                            }
                          }
                          """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          deleteOneCommandResolver.resolveCommand(commandContext, deleteOneCommand);
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(new FindOperation.IDFilter(FindOperation.IDFilter.Operator.EQ, "id")),
              null,
              1,
              1,
              false,
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
                      "deleteOne": {
                      }
                    }
                    """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          deleteOneCommandResolver.resolveCommand(commandContext, deleteOneCommand);
      FindOperation findOperation =
          new FindOperation(commandContext, List.of(), null, 1, 1, false, objectMapper);
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
                      "deleteOne": {
                        "filter" : {"col" : "val"}
                      }
                    }
                    """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          deleteOneCommandResolver.resolveCommand(commandContext, deleteOneCommand);
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new FindOperation.TextFilter(
                      "col", FindOperation.MapFilterBase.Operator.EQ, "val")),
              null,
              1,
              1,
              false,
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
