package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.DBFilterBase;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FilterMatchRulesTest {
  @Inject ObjectMapper objectMapper;
  private List<DBFilterBase> filters = mock(List.class);

  @Nested
  class FilterMatchRulesApply {
    @Test
    public void apply() throws Exception {
      String json =
          """
                        {
                          "findOne": {
                            "sort" : {"user.name" : 1, "user.age" : -1},
                            "filter" : {"col" : "val"}
                          }
                        }
                        """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      FilterMatchRules filterMatchRules = new FilterMatchRules<FindOneCommand>();

      Function<CaptureExpression, List<DBFilterBase>> resolveFunction =
          captureExpression -> filters;

      filterMatchRules
          .addMatchRule(resolveFunction, FilterMatcher.MatchStrategy.EMPTY)
          .matcher()
          .capture("EMPTY");
      filterMatchRules
          .addMatchRule(resolveFunction, FilterMatcher.MatchStrategy.GREEDY)
          .matcher()
          .capture("TEST1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);

      LogicalExpression response = filterMatchRules.apply(TestConstants.CONTEXT, findOneCommand);
      assertThat(response).isNotNull();

      json =
          """
          {
            "findOne": {
              "sort" : {"user.name" : 1, "user.age" : -1}
            }
          }
          """;

      findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      response = filterMatchRules.apply(TestConstants.CONTEXT, findOneCommand);
      assertThat(response).isNotNull();
    }

    @Test
    public void addMatchRule() throws Exception {
      String json =
          """
              {
                "findOne": {
                  "sort" : {"user.name" : 1, "user.age" : -1},
                  "filter" : {"col" : "val"}
                }
              }
              """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      FilterMatchRules filterMatchRules = new FilterMatchRules<FindOneCommand>();
      Function<CaptureExpression, List<DBFilterBase>> resolveFunction =
          captureExpression -> filters;
      filterMatchRules
          .addMatchRule(resolveFunction, FilterMatcher.MatchStrategy.EMPTY)
          .matcher()
          .capture("EMPTY");
      filterMatchRules
          .addMatchRule(resolveFunction, FilterMatcher.MatchStrategy.GREEDY)
          .matcher()
          .capture("TEST1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);

      assertThat(filterMatchRules.getMatchRules()).hasSize(2);
    }
  }
}
