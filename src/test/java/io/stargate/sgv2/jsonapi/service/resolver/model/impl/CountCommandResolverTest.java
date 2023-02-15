package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.CountOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CountCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject CountCommandResolver countCommandResolver;

  @Test
  public void noFilterCondition() throws Exception {
    String json = """
          {
            "count": {

            }
          }
          """;

    CountCommand countCommand = objectMapper.readValue(json, CountCommand.class);
    final CommandContext commandContext = new CommandContext("namespace", "collection");
    final Operation operation = countCommandResolver.resolveCommand(commandContext, countCommand);
    CountOperation expected = new CountOperation(commandContext, List.of());
    assertThat(operation)
        .isInstanceOf(CountOperation.class)
        .satisfies(op -> assertThat(op).isEqualTo(expected));
  }

  @Test
  public void dynamicFilterCondition() throws Exception {
    String json =
        """
                {
                  "count": {
                    "filter" : {"col" : "val"}
                  }
                }
                """;

    CountCommand countCommand = objectMapper.readValue(json, CountCommand.class);
    final CommandContext commandContext = new CommandContext("namespace", "collection");
    final Operation operation = countCommandResolver.resolveCommand(commandContext, countCommand);
    CountOperation expected =
        new CountOperation(
            commandContext,
            List.of(
                new DBFilterBase.TextFilter("col", DBFilterBase.MapFilterBase.Operator.EQ, "val")));
    assertThat(operation)
        .isInstanceOf(CountOperation.class)
        .satisfies(op -> assertThat(op).isEqualTo(expected));
  }
}
