package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import java.util.EnumSet;
import java.util.function.BiFunction;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FilterMatchRulesTest {
  @Inject ObjectMapper objectMapper;
  private ReadOperation readOperation = mock(ReadOperation.class);

  @Nested
  class FilterMatchRulesApply {
    @Test
    public void apply() throws Exception {
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
      FilterMatchRules filterMatchRules = new FilterMatchRules<FindOneCommand>();
      BiFunction<CommandContext, CaptureGroups<FindOneCommand>, ReadOperation> resolveFunction =
          (commandContext, captures) -> readOperation;
      filterMatchRules
          .addMatchRule(resolveFunction, FilterMatcher.MatchStrategy.EMPTY)
          .matcher()
          .capture("EMPTY");
      filterMatchRules
          .addMatchRule(resolveFunction, FilterMatcher.MatchStrategy.GREEDY)
          .matcher()
          .capture("TEST1")
          .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);

      ReadOperation response =
          filterMatchRules.apply(new CommandContext("database", "collection"), findOneCommand);
      assertThat(response).isNotNull();

      json =
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

      findOneCommand = objectMapper.readValue(json, FindOneCommand.class);
      response =
          filterMatchRules.apply(new CommandContext("database", "collection"), findOneCommand);
      assertThat(response).isNotNull();
    }

    @Test
    public void addMatchRule() throws Exception {
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
      FilterMatchRules filterMatchRules = new FilterMatchRules<FindOneCommand>();
      BiFunction<CommandContext, CaptureGroups<FindOneCommand>, ReadOperation> resolveFunction =
          (commandContext, captures) -> readOperation;
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
