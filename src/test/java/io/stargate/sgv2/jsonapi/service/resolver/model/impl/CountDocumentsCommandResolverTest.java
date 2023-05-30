package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.Mock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommands;
import io.stargate.sgv2.jsonapi.service.operation.model.CountOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase.MapFilterBase.Operator;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CountDocumentsCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CountDocumentsCommandResolver resolver;

  @Nested
  class ResolveCommand {

    @Mock CommandContext context;

    @Test
    public void noFilter() throws Exception {
      String json =
          """
            {
              "countDocuments": {
              }
            }
            """;

      CountDocumentsCommands command = objectMapper.readValue(json, CountDocumentsCommands.class);
      Operation result = resolver.resolveCommand(context, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CountOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(context);
                assertThat(op.filters()).isEmpty();
              });
    }

    @Test
    public void withFilter() throws Exception {
      String json =
          """
            {
              "countDocuments": {
                "filter": {
                    "name": "Aaron"
                }
              }
            }
            """;

      CountDocumentsCommands command = objectMapper.readValue(json, CountDocumentsCommands.class);
      Operation result = resolver.resolveCommand(context, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CountOperation.class,
              op -> {
                DBFilterBase.TextFilter expected =
                    new DBFilterBase.TextFilter("name", Operator.EQ, "Aaron");

                assertThat(op.commandContext()).isEqualTo(context);
                assertThat(op.filters()).singleElement().isEqualTo(expected);
              });
    }
  }
}
