package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FilterMatchRuleTest {
  @Inject ObjectMapper objectMapper;
  private TestConstants testConstants = new TestConstants();

  @Nested
  class FilterMatchRuleApply {
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
      BiFunction<DBLogicalExpression, CaptureGroups, DBLogicalExpression> resolveFunction =
          (dbLogicalExpression, captureGroups) -> dbLogicalExpression;
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher
          .capture("CAPTURE 1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);

      FilterMatchRule<FindOneCommand> filterMatchRule =
          new FilterMatchRule(matcher, resolveFunction);
      Optional<DBLogicalExpression> response =
          filterMatchRule.apply(testConstants.collectionContext(), findOneCommand);
      assertThat(response).isPresent();

      matcher = new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher
          .capture("CAPTURE 1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL);
      filterMatchRule = new FilterMatchRule(matcher, resolveFunction);

      response = filterMatchRule.apply(testConstants.collectionContext(), findOneCommand);
      assertThat(response).isEmpty();
    }

    @Test
    public void testDynamicIn() throws Exception {
      String json =
          """
                  {
                    "findOne": {
                      "filter" : {"name" : {"$in" : ["testname1", "testname2"]}}
                    }
                  }
                  """;
      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      BiFunction<DBLogicalExpression, CaptureGroups, DBLogicalExpression> resolveFunction =
          (dbLogicalExpression, captureGroups) -> dbLogicalExpression;
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);

      FilterMatchRule<FindOneCommand> filterMatchRule =
          new FilterMatchRule(matcher, resolveFunction);

      filterMatchRule
          .matcher()
          .capture("capture marker")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY);

      Optional<DBLogicalExpression> response =
          filterMatchRule.apply(testConstants.collectionContext(), findOneCommand);
      assertThat(response).isPresent();
    }
  }
}
