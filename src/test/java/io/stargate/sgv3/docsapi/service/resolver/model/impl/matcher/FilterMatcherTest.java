package io.stargate.sgv3.docsapi.service.resolver.model.impl.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindOneCommand;
import java.util.Optional;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FilterMatcherTest {
  @Inject ObjectMapper objectMapper;

  @Nested
  class FilterMatcherEmptyApply {
    @Test
    public void applyWithNoFilter() throws Exception {
      String json =
          """
            {
              "findOne": {
              }
            }
            """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.EMPTY);
      final Optional<CaptureGroups<FindOneCommand>> response = matcher.apply(findOneCommand);
      assertThat(response.isPresent()).isTrue();
    }

    @Test
    public void applyWithFilter() throws Exception {
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
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.EMPTY);
      final Optional<CaptureGroups<FindOneCommand>> response = matcher.apply(findOneCommand);
      assertThat(response.isPresent()).isFalse();
    }
  }

  @Nested
  class FilterMatcherStrictApply {
    @Test
    public void applyWithNoFilter() throws Exception {
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
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.STRICT);
      matcher.capture("TEST").compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING);
      final Optional<CaptureGroups<FindOneCommand>> response = matcher.apply(findOneCommand);
      assertThat(response.isPresent()).isFalse();
    }

    @Test
    public void applyWithFilterMatch() throws Exception {
      String json =
          """
            {
              "findOne": {
                "sort": [
                  "user.name",
                  "-user.age"
                ],
                "filter" : {"col" : "val", "col2" : 10}
              }
            }
            """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.STRICT);
      matcher.capture("CAPTURE 1").compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING);
      matcher.capture("CAPTURE 2").compareValues("*", ValueComparisonOperator.EQ, JsonType.NUMBER);
      final Optional<CaptureGroups<FindOneCommand>> response = matcher.apply(findOneCommand);
      assertThat(response.isPresent()).isTrue();
    }

    @Test
    public void applyWithFilterNotMatch() throws Exception {
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
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.STRICT);
      matcher.capture("CAPTURE 1").compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING);
      matcher.capture("CAPTURE 2").compareValues("*", ValueComparisonOperator.EQ, JsonType.NUMBER);
      final Optional<CaptureGroups<FindOneCommand>> response = matcher.apply(findOneCommand);
      assertThat(response.isPresent()).isFalse();
    }
  }

  @Nested
  class FilterMatcherGreedyApply {
    @Test
    public void applyWithNoFilter() throws Exception {
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
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher.capture("TEST").compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING);
      final Optional<CaptureGroups<FindOneCommand>> response = matcher.apply(findOneCommand);
      assertThat(response.isPresent()).isFalse();
    }

    @Test
    public void applyWithFilterMatch() throws Exception {
      String json =
          """
            {
              "findOne": {
                "sort": [
                  "user.name",
                  "-user.age"
                ],
                "filter" : {"col" : "val", "col2" : 10}
              }
            }
            """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher.capture("CAPTURE 1").compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING);
      matcher.capture("CAPTURE 2").compareValues("*", ValueComparisonOperator.EQ, JsonType.NUMBER);
      final Optional<CaptureGroups<FindOneCommand>> response = matcher.apply(findOneCommand);
      assertThat(response.isPresent()).isTrue();
    }

    @Test
    public void applyWithFilterNoMatch() throws Exception {
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
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher.capture("CAPTURE 1").compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING);
      matcher.capture("CAPTURE 2").compareValues("*", ValueComparisonOperator.EQ, JsonType.NUMBER);
      final Optional<CaptureGroups<FindOneCommand>> response = matcher.apply(findOneCommand);
      assertThat(response.isPresent()).isTrue();
    }
  }
}
