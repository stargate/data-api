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
import javax.inject.Inject;
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
          [
           "some.path",
           "-another.path"
          ]
          """;

      SortClause sortClause = objectMapper.readValue(json, SortClause.class);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions())
          .hasSize(2)
          .contains(
              new SortExpression("some.path", true), new SortExpression("another.path", false));
    }

    @Test
    public void mustTrimPath() throws Exception {
      String json = """
          ["some.path "]
          """;

      SortClause sortClause = objectMapper.readValue(json, SortClause.class);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions())
          .hasSize(1)
          .contains(new SortExpression("some.path", true));
    }

    @Test
    public void mustHandleNull() throws Exception {
      String json = "null";

      SortClause sortClause = objectMapper.readValue(json, SortClause.class);

      assertThat(sortClause).isNull();
    }

    @Test
    public void mustBeArray() {
      String json = """
                    "primitive"
                    """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonMappingException.class);
    }

    @Test
    public void mustBeCorrectContainerNode() {
      String json = """
                    {"path": "some"}
                    """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonMappingException.class);
    }

    @Test
    public void mustContainString() {
      String json = "[2]";

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonMappingException.class);
    }

    @Test
    public void mustNotContainBlankString() {
      String json = """
          [" "]
          """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonMappingException.class);
    }

    @Test
    public void mustNotContainPathAfterMinus() {
      String json = """
          ["-"]
          """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, SortClause.class));

      assertThat(throwable).isInstanceOf(JsonMappingException.class);
    }
  }
}
