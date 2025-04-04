package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FilterMatcherTest {
  @Inject ObjectMapper objectMapper;
  private TestConstants testConstants = new TestConstants();

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
      final Optional<CaptureGroups<FindOneCommand>> response =
          matcher.apply(testConstants.collectionContext(), findOneCommand);
      assertThat(response.isPresent()).isTrue();
    }

    @Test
    public void applyWithFilter() throws Exception {
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
          new FilterMatcher<>(FilterMatcher.MatchStrategy.EMPTY);
      final Optional<CaptureGroups<FindOneCommand>> response =
          matcher.apply(testConstants.collectionContext(), findOneCommand);
      assertThat(response.isPresent()).isFalse();
    }

    @Nested
    class FilterMatcherStrictApply {
      @Test
      public void applyWithNoFilter() throws Exception {
        String json =
            """
                          {
                            "findOne": {
                              "sort" : {"user.name" : 1, "user.age" : -1}
                            }
                          }
                          """;

        FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
        FilterMatcher<FindOneCommand> matcher =
            new FilterMatcher<>(FilterMatcher.MatchStrategy.STRICT);
        matcher
            .capture("TEST")
            .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);
        final Optional<CaptureGroups<FindOneCommand>> response =
            matcher.apply(testConstants.collectionContext(), findOneCommand);
        assertThat(response.isPresent()).isFalse();
      }
    }

    @Test
    public void applyWithFilterMatch() throws Exception {
      String json =
          """
                {
                  "findOne": {
                    "sort" : {"user.name" : 1, "user.age" : -1},
                    "filter" : {"col" : "val", "col2" : 10}
                  }
                }
                """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.STRICT);
      matcher
          .capture("CAPTURE 1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);
      matcher
          .capture("CAPTURE 2")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NUMBER);
      final Optional<CaptureGroups<FindOneCommand>> response =
          matcher.apply(testConstants.collectionContext(), findOneCommand);
      assertThat(response.isPresent()).isTrue();
    }

    @Test
    public void applyWithFilterNotMatch() throws Exception {
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
          new FilterMatcher<>(FilterMatcher.MatchStrategy.STRICT);
      matcher
          .capture("CAPTURE 1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);
      matcher
          .capture("CAPTURE 2")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NUMBER);
      final Optional<CaptureGroups<FindOneCommand>> response =
          matcher.apply(testConstants.collectionContext(), findOneCommand);
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
                              "sort" : {"user.name" : 1, "user.age" : -1}
                            }
                          }
                          """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher
          .capture("TEST")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);
      final Optional<CaptureGroups<FindOneCommand>> response =
          matcher.apply(testConstants.collectionContext(), findOneCommand);
      // Missing filter now same as empty, so:
      assertThat(response.isPresent()).isTrue();
    }

    @Test
    public void applyWithFilterMatch() throws Exception {
      String json =
          """
                          {
                            "findOne": {
                              "sort" : {"user.name" : 1, "user.age" : -1},
                              "filter" : {"col" : "val", "col2" : 10}
                            }
                          }
                          """;

      FindOneCommand findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      FilterMatcher<FindOneCommand> matcher =
          new FilterMatcher<>(FilterMatcher.MatchStrategy.GREEDY);
      matcher
          .capture("CAPTURE 1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);
      matcher
          .capture("CAPTURE 2")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NUMBER);
      final Optional<CaptureGroups<FindOneCommand>> response =
          matcher.apply(testConstants.collectionContext(), findOneCommand);
      assertThat(response.isPresent()).isTrue();
    }

    @Test
    public void applyWithFilterNoMatch() throws Exception {
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
      matcher
          .capture("CAPTURE 2")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NUMBER);
      final Optional<CaptureGroups<FindOneCommand>> response =
          matcher.apply(testConstants.collectionContext(), findOneCommand);
      assertThat(response.isPresent()).isTrue();
    }
  }
}
