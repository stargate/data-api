package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommands;
import io.stargate.sgv2.jsonapi.service.operation.model.CountOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CountCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject CountDocumentsCommandResolver countCommandResolver;

  @Test
  public void noFilterCondition() throws Exception {
    String json =
        """
          {
            "countDocuments": {

            }
          }
          """;

    CountDocumentsCommands countCommand =
        objectMapper.readValue(json, CountDocumentsCommands.class);
    final CommandContext commandContext = new CommandContext("namespace", "collection");
    final Operation operation = countCommandResolver.resolveCommand(commandContext, countCommand);
    LogicalExpression implicitAnd = LogicalExpression.and();
    CountOperation expected = new CountOperation(commandContext, implicitAnd);
    assertThat(operation)
        .isInstanceOf(CountOperation.class)
        .satisfies(
            op -> {
              CountOperation countOperation = (CountOperation) op;
              assertThat(countOperation.logicalExpression().getTotalComparisonExpressionCount())
                  .isEqualTo(expected.logicalExpression().getTotalComparisonExpressionCount());
            });
  }

  @Test
  public void dynamicFilterCondition() throws Exception {
    String json =
        """
                {
                  "countDocuments": {
                    "filter" : {"col" : "val"}
                  }
                }
                """;

    CountDocumentsCommands countCommand =
        objectMapper.readValue(json, CountDocumentsCommands.class);
    final CommandContext commandContext = new CommandContext("namespace", "collection");
    final Operation operation = countCommandResolver.resolveCommand(commandContext, countCommand);

    LogicalExpression implicitAnd = LogicalExpression.and();
    implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
    implicitAnd
        .comparisonExpressions
        .get(0)
        .setDBFilters(
            List.of(
                new DBFilterBase.TextFilter("col", DBFilterBase.MapFilterBase.Operator.EQ, "val")));
    CountOperation expected = new CountOperation(commandContext, implicitAnd);
    assertThat(operation)
        .isInstanceOf(CountOperation.class)
        .satisfies(
            op -> {
              CountOperation countOperation = (CountOperation) op;
              assertThat(
                      countOperation
                          .logicalExpression()
                          .comparisonExpressions
                          .get(0)
                          .getDbFilters())
                  .isEqualTo(
                      expected.logicalExpression().comparisonExpressions.get(0).getDbFilters());
            });
  }
}
