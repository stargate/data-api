package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FilterMatchRuleTest {
  @Inject ObjectMapper objectMapper;
  private List<DBFilterBase> filters = mock(List.class);

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
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher
          .capture("CAPTURE 1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);
      BiFunction<CommandContext, CaptureGroups<FindOneCommand>, List<DBFilterBase>>
          resolveFunction =
              (commandContext, captures) -> {
                return filters;
              };

      FilterMatchRule<FindOneCommand> filterMatchRule =
          new FilterMatchRule(matcher, resolveFunction);
      Optional<List<DBFilterBase>> response =
          filterMatchRule.apply(new CommandContext("namespace", "collection"), findOneCommand);
      assertThat(response).isPresent();

      matcher = new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher
          .capture("CAPTURE 1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL);
      filterMatchRule = new FilterMatchRule(matcher, resolveFunction);
      response =
          filterMatchRule.apply(new CommandContext("namespace", "collection"), findOneCommand);
      assertThat(response).isEmpty();
    }
  }
}
