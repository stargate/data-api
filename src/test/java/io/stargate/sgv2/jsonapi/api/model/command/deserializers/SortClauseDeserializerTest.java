package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class SortClauseDeserializerTest {

  @Inject ObjectMapper objectMapper;

  @Nested
  class Deserialize {

    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
           "some.path" : 1,
           "another.path" : -1
          }
          """;

      SortClause sortClause = objectMapper.readValue(json, SortClause.class);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions())
          .hasSize(2)
          .contains(
              SortExpression.sort("some.path", true), SortExpression.sort("another.path", false));
    }

    @Test
    public void happyPathVectorSearch() throws Exception {
      String json = """
        {
         "$vector" : [0.11, 0.22, 0.33]
        }
        """;

      SortClause sortClause = objectMapper.readValue(json, SortClause.class);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions()).hasSize(1);
      assertThat(sortClause.sortExpressions().get(0).path()).isEqualTo("$vector");
      assertThat(sortClause.sortExpressions().get(0).vector())
          .containsExactly(new Float[] {0.11f, 0.22f, 0.33f});
    }

    @Test
    public void vectorSearchEmpty() {
      String json = """
        {
         "$vector" : []
        }
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage()).contains("$vector field can't be empty");
    }

    @Test
    public void vectorSearchNonArray() {
      String json = """
        {
         "$vector" : 0.55
        }
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage()).contains("$vector search needs to be array of numbers");
    }

    @Test
    public void vectorSearchNonArrayObject() {
      String json = """
        {
         "$vector" : {}
        }
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage()).contains("$vector search needs to be array of numbers");
    }

    public void vectorSearchInvalidData() {
      String json = """
        {
         "$vector" : [0.11, "abc", true]
        }
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage()).contains("$vector search needs to be array of numbers");
    }

    public void vectorSearchInvalidSortClause() {
      String json =
          """
        {
         "$vector" : [0.11, 0.22, 0.33],
         "some.path" : 1
        }
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage())
          .contains("Vector search can't be used with other sort clause");
    }

    @Test
    public void mustTrimPath() throws Exception {
      String json = """
              {"some.path " : 1}
          """;

      SortClause sortClause = objectMapper.readValue(json, SortClause.class);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions())
          .hasSize(1)
          .contains(SortExpression.sort("some.path", true));
    }

    @Test
    public void mustHandleNull() throws Exception {
      String json = "null";

      SortClause sortClause = objectMapper.readValue(json, SortClause.class);

      assertThat(sortClause).isNull();
    }

    @Test
    public void mustBeObject() {
      String json = """
                    ["primitive"]
                    """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonMappingException.class);
    }

    @Test
    public void mustBeCorrectContainerNode() {
      String json = """
                    {"path": "value"}
                    """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
    }

    @Test
    public void mustNotContainBlankString() {
      String json = """
              {" " : 1}
          """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
    }

    @Test
    public void mustNotContainEmptyString() {
      String json = """
              {"": 1}
          """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
    }
  }
}
