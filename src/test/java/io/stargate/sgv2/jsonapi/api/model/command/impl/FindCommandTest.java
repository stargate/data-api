package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindCommandTest {
  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Find {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
        {
          "find": {
            "filter" : {"username" : "user1"},
            "sort" : {"username" : 1},
            "options" : {
              "limit" : 1,
              "skip": 100
            }
          }
        }
        """;

      FindCommand command = objectMapper.readValue(json, FindCommand.class);
      Set<ConstraintViolation<FindCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }

    @Test
    public void invalidOptionsNegativeLimit() throws Exception {
      String json =
          """
            {
            "find": {
                "filter" : {"username" : "user1"},
                "options" : {
                  "limit" : -1
                }
              }
            }
            """;

      FindCommand command = objectMapper.readValue(json, FindCommand.class);
      Set<ConstraintViolation<FindCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("limit should be greater than `0`");
    }

    @Test
    public void invalidOptionsZeroLimit() throws Exception {
      String json =
          """
            {
            "find": {
                "filter" : {"username" : "user1"},
                "options" : {
                  "limit" : 0
                }
              }
            }
            """;

      FindCommand command = objectMapper.readValue(json, FindCommand.class);
      Set<ConstraintViolation<FindCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("limit should be greater than `0`");
    }

    @Test
    public void invalidOptionsNegativeSkip() throws Exception {
      String json =
          """
          {
          "find": {
              "sort" : {"username" : 1},
              "options" : {
                "skip" : -1
              }
            }
          }
          """;

      FindCommand command = objectMapper.readValue(json, FindCommand.class);
      Set<ConstraintViolation<FindCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("skip should be greater than or equal to `0`");
    }

    @Test
    public void invalidOptionsSkipNoSort() throws Exception {
      String json =
          """
          {
          "find": {
              "filter" : {"username" : "user1"},
              "options" : {
                "skip" : 10
              }
            }
          }
          """;

      FindCommand command = objectMapper.readValue(json, FindCommand.class);
      Set<ConstraintViolation<FindCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("skip options should be used with sort clause");
    }

    @Test
    public void invalidOptionsPagingStateWithSort() throws Exception {
      String json =
          """
          {
           "find": {
              "filter" : {"username" : "user1"},
              "sort" : {"username" : 1},
              "options" : {
                "pagingState" : "someState"
              }
            }
          }
          """;

      FindCommand command = objectMapper.readValue(json, FindCommand.class);
      Set<ConstraintViolation<FindCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("pagingState is not supported with sort clause");
    }

    @Test
    public void invalidOptionsSkipVectorSearch() throws Exception {
      String json =
          """
        {
        "find": {
            "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
            "options" : {
              "skip" : 10
            }
          }
        }
        """;

      FindCommand command = objectMapper.readValue(json, FindCommand.class);
      Set<ConstraintViolation<FindCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("skip options should not be used with vector search");
    }

    @Test
    public void invalidOptionsLimitVectorSearch() throws Exception {
      String json =
          """
              {
              "find": {
                  "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
                  "options" : {
                    "limit" : 1001
                  }
                }
              }
              """;

      FindCommand command = objectMapper.readValue(json, FindCommand.class);
      Set<ConstraintViolation<FindCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("limit options should not be greater than 1000 for vector search");
    }
  }
}
