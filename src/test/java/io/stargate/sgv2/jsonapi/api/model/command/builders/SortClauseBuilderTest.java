package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.util.Base64Util;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import jakarta.inject.Inject;
import java.io.IOException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class SortClauseBuilderTest {
  // Needed to create the collection context to pass to the builder
  private final TestConstants testConstants = new TestConstants();

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

      SortClause sortClause = deserializeSortClause(json);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions())
          .hasSize(2)
          .contains(
              SortExpression.sort("some.path", true), SortExpression.sort("another.path", false));
    }

    @Test
    public void happyPathWithUnusualChars() throws Exception {
      String json =
          """
              {
               "app.kubernetes.io/name" : 1,
               "another.odd$path" : -1
              }
              """;

      SortClause sortClause = deserializeSortClause(json);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions())
          .hasSize(2)
          .contains(
              SortExpression.sort("app.kubernetes.io/name", true),
              SortExpression.sort("another.odd$path", false));
    }

    @Test
    public void happyPathWithEscapedChars() throws Exception {
      // should have the escape character in the expression
      String json =
          """
              {
               "app&.kubernetes&.io/name" : 1,
               "another&.odd&&path" : -1
              }
              """;

      SortClause sortClause = deserializeSortClause(json);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions())
          .hasSize(2)
          .containsExactlyInAnyOrder(
              SortExpression.sort("app&.kubernetes&.io/name", true),
              SortExpression.sort("another&.odd&&path", false))
          .doesNotContainSequence(
              SortExpression.sort("app.kubernetes.io/name", true),
              SortExpression.sort("another.odd&path", false));
    }

    @Test
    public void happyPathVectorSearch() throws Exception {
      String json =
          """
        {
         "$vector" : [0.11, 0.22, 0.33]
        }
        """;

      SortClause sortClause = deserializeSortClause(json);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions()).hasSize(1);
      assertThat(sortClause.sortExpressions().get(0).getPath()).isEqualTo("$vector");
      assertThat(sortClause.sortExpressions().get(0).getVector())
          .containsExactly(new Float[] {0.11f, 0.22f, 0.33f});
    }

    @Test
    public void vectorSearchBinaryObject() throws Exception {
      String vectorString =
          Base64Util.encodeAsMimeBase64(
              CqlVectorUtil.floatsToBytes(new float[] {0.11f, 0.22f, 0.33f}));
      String json =
              """
            {
             "$vector" : { "$binary" : "%s"}
            }
            """
              .formatted(vectorString);

      SortClause sortClause = deserializeSortClause(json);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions()).hasSize(1);
      assertThat(sortClause.sortExpressions().getFirst().getPath()).isEqualTo("$vector");
      assertThat(sortClause.sortExpressions().getFirst().getVector())
          .containsExactly(new Float[] {0.11f, 0.22f, 0.33f});
    }

    @Test
    public void binaryVectorSearchTableColumn() throws Exception {
      String vectorString =
          Base64Util.encodeAsMimeBase64(
              CqlVectorUtil.floatsToBytes(new float[] {0.11f, 0.22f, 0.33f}));
      String json =
              """
            {
             "$vector" : { "$binary" : "%s"}
            }
            """
              .formatted(vectorString);

      SortClause sortClause = deserializeSortClause(json);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions()).hasSize(1);
      assertThat(sortClause.sortExpressions().get(0).getPath()).isEqualTo("$vector");
      assertThat(sortClause.sortExpressions().get(0).getVector())
          .containsExactly(new Float[] {0.11f, 0.22f, 0.33f});
    }

    @Test
    public void vectorSearchEmpty() {
      String json =
          """
        {
         "$vector" : []
        }
        """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage()).contains("$vector value can't be empty");
    }

    @Test
    public void vectorSearchNonArray() {
      String json =
          """
        {
         "$vector" : 0.55
        }
        """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage()).contains("$vector value needs to be array of numbers");
    }

    @Test
    public void vectorSearchNonArrayObject() {
      String json =
          """
        {
         "$vector" : {}
        }
        """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage())
          .contains(
              "Invalid sort clause value: Only binary vector object values is supported for sorting. Path: $vector, Value: {}.");
    }

    @Test
    public void vectorSearchInvalidData() {
      String json =
          """
        {
         "$vector" : [0.11, "abc", true]
        }
        """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage()).contains("$vector value needs to be array of numbers");
    }

    @Test
    public void vectorSearchInvalidSortClause() {
      String json =
          """
        {
         "$vector" : [0.11, 0.22, 0.33],
         "some.path" : 1
        }
        """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage())
          .contains("Vector search can't be used with other sort clause");
    }

    @Test
    public void happyPathVectorizeSearch() throws Exception {
      String json =
          """
        {
         "$vectorize" : "test data"
        }
        """;

      SortClause sortClause = deserializeSortClause(json);

      assertThat(sortClause).isNotNull();
      assertThat(sortClause.sortExpressions()).hasSize(1);
      assertThat(sortClause.sortExpressions().get(0).getPath()).isEqualTo("$vectorize");
      assertThat(sortClause.sortExpressions().get(0).getVectorize()).isEqualTo("test data");
    }

    @Test
    public void vectorizeSearchNonText() {
      String json =
          """
        {
         "$vectorize" : 0.55
        }
        """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage())
          .contains("$vectorize search clause needs to be non-blank text value");
    }

    @Test
    public void vectorizeSearchObject() {
      String json =
          """
        {
         "$vectorize" : {}
        }
        """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage())
          .contains("$vectorize search clause needs to be non-blank text value");
    }

    @Test
    public void vectorizeSearchBlank() {
      String json =
          """
            {
             "$vectorize" : " "
            }
            """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage())
          .contains("$vectorize search clause needs to be non-blank text value");
    }

    @Test
    public void vectorizeSearchWithOtherSort() {
      String json =
          """
        {
         "$vectorize" : "test data",
         "some.path" : 1
        }
        """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable.getMessage())
          .contains("Vector search can't be used with other sort clause");
    }

    @Test
    public void mustHandleNull() throws Exception {
      String json = "null";

      SortClause sortClause = deserializeSortClause(json);

      // Note: we will always create non-null sort clause
      assertThat(sortClause).isNotNull();
      assertThat(sortClause.isEmpty()).isTrue();
    }

    @Test
    public void mustBeObject() {
      String json =
          """
                    ["primitive"]
                    """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
    }

    @Test
    public void mustNotContainBlankString() {
      String json =
          """
              {" " : 1}
          """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
    }

    @Test
    public void mustNotContainEmptyString() {
      String json =
          """
              {"": 1}
          """;

      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
    }

    @Test
    public void invalidPathNameOperator() {
      String json =
          """
              {"$gt": 1}
          """;
      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable)
          .hasMessageContaining("Invalid sort clause path: path ('$gt') cannot start with '$'");
    }

    // [data-api#1967] - Not allowed to use "$hybrid"; either with 1/-1 or with String
    @Test
    public void invalidPathNameHybridWithNumber() {
      // First test with regular 1/-1 value
      Throwable t =
          catchThrowable(
              () ->
                  deserializeSortClause(
                      """
                          {"$hybrid": 1}
                      """));

      assertThat(t).isInstanceOf(JsonApiException.class);
      assertThat(t)
          .hasMessageContaining("Invalid sort clause path: path ('$hybrid') cannot start with '$'");
    }

    // [data-api#1967] - Not allowed to use "$hybrid"; either with 1/-1 or with String
    @Test
    public void invalidPathNameHybridWithString() {
      Throwable t =
          catchThrowable(
              () ->
                  deserializeSortClause(
                      """
                  {"$hybrid": "tokens are tasty"}
              """));

      assertThat(t).isInstanceOf(JsonApiException.class);
      assertThat(t)
          .hasMessageContaining("Invalid sort clause path: path ('$hybrid') cannot start with '$'");
    }

    @Test
    public void invalidEscapeUsage() {
      String json =
          """
          {"a&b": 1}
          """;
      Throwable throwable = catchThrowable(() -> deserializeSortClause(json));

      assertThat(throwable).isInstanceOf(JsonApiException.class);
      assertThat(throwable)
          .hasMessageContaining(
              "Invalid sort clause path: sort clause path ('a&b') is not a valid path.");
    }
  }

  private SortClause deserializeSortClause(String json) throws IOException {
    final JsonNode node = objectMapper.readTree(json);
    CollectionSchemaObject schema = testConstants.collectionContext().schemaObject();
    return SortClauseBuilder.builderFor(schema).build(node);
  }
}
