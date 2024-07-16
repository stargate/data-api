package io.stargate.sgv2.jsonapi.service.cql.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Variable;
import com.datastax.oss.driver.api.core.data.CqlVector;
import io.stargate.sgv2.jsonapi.service.cql.ExpressionUtils;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.JsonTerm;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryBuilderTest {

  public static final String VECTOR_COLUMN = "query_vector_value";
  public static final float[] TEST_VECTOR = new float[] {0.1f, 0.2f, 0.3f};

  public static final CqlVector<Float> TEST_CQL_VECTOR = CQLBindValues.getVectorValue(TEST_VECTOR);

  public static final List<Object> EMPTY_VALUES = new ArrayList<>();

  @ParameterizedTest
  @MethodSource("sampleQueries")
  @DisplayName("Should generate expected CQL string and values")
  public void generateQuery(Query query, String expectedCql, List<Object> expectedValues) {
    assertThat(query.cql()).isEqualTo(expectedCql);
    assertThat(query.values()).isEqualTo(expectedValues);
  }

  public static Arguments[] sampleQueries() {
    return new Arguments[] {
      arguments(
          new QueryBuilder().select().from("ks", "tbl").build(),
          "SELECT * FROM ks.tbl",
          EMPTY_VALUES),
      arguments(
          new QueryBuilder().select().column("a", "b", "c").from("ks", "tbl").build(),
          "SELECT a, b, c FROM ks.tbl",
          EMPTY_VALUES),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").build(),
          "SELECT COUNT(a) FROM ks.tbl",
          EMPTY_VALUES),
      arguments(
          new QueryBuilder().select().count().from("ks", "tbl").build(),
          "SELECT COUNT(1) FROM ks.tbl",
          EMPTY_VALUES),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").limit(1).build(),
          "SELECT COUNT(a) FROM ks.tbl LIMIT 1",
          EMPTY_VALUES),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").limit().build(),
          "SELECT COUNT(a) FROM ks.tbl LIMIT ?",
          EMPTY_VALUES),
      arguments(
          new QueryBuilder()
              .select()
              .column("FirstName", "b", "c")
              .from("ks", "tbl")
              .limit(1)
              .build(),
          "SELECT \"FirstName\", b, c FROM ks.tbl LIMIT 1",
          EMPTY_VALUES,
          arguments(
              new QueryBuilder()
                  .select()
                  .column("a", "b", "c")
                  .from("ks", "tbl")
                  .limit(1)
                  .vsearch(VECTOR_COLUMN, TEST_VECTOR)
                  .build(),
              "SELECT a, b, c FROM ks.tbl ORDER BY query_vector_value ANN OF ? LIMIT 1",
              List.of(TEST_CQL_VECTOR)),
          arguments(
              new QueryBuilder()
                  .select()
                  .column("a", "b", "c")
                  .similarityFunction(
                      "query_vector_value", CollectionSchemaObject.SimilarityFunction.COSINE)
                  .from("ks", "tbl")
                  .limit(1)
                  .vsearch(VECTOR_COLUMN, TEST_VECTOR)
                  .build(),
              "SELECT a, b, c, SIMILARITY_COSINE(query_vector_value, ?) FROM ks.tbl ORDER BY query_vector_value ANN OF ? LIMIT 1",
              List.of(TEST_CQL_VECTOR, TEST_CQL_VECTOR)),
          arguments(
              new QueryBuilder()
                  .select()
                  .column("a", "b", "c")
                  .similarityFunction(
                      "query_vector_value", CollectionSchemaObject.SimilarityFunction.DOT_PRODUCT)
                  .from("ks", "tbl")
                  .limit(1)
                  .vsearch(VECTOR_COLUMN, TEST_VECTOR)
                  .build(),
              "SELECT a, b, c, SIMILARITY_DOT_PRODUCT(query_vector_value, ?) FROM ks.tbl ORDER BY query_vector_value ANN OF ? LIMIT 1",
              List.of(TEST_CQL_VECTOR, TEST_CQL_VECTOR)),
          arguments(
              new QueryBuilder()
                  .select()
                  .column("a", "b", "c")
                  .similarityFunction(
                      VECTOR_COLUMN, CollectionSchemaObject.SimilarityFunction.EUCLIDEAN)
                  .from("ks", "tbl")
                  .limit(1)
                  .vsearch("query_vector_value", TEST_VECTOR)
                  .build(),
              "SELECT a, b, c, SIMILARITY_EUCLIDEAN(query_vector_value, ?) FROM ks.tbl ORDER BY query_vector_value ANN OF ? LIMIT 1",
              List.of(TEST_CQL_VECTOR, TEST_CQL_VECTOR)))
    };
  }

  @Nested
  public class expressionToCqlBuilderTest {
    @Test
    public void simpleAnd() {
      Expression<BuiltCondition> expression =
          ExpressionUtils.andOf(
              Variable.of(
                  BuiltCondition.of("Name", BuiltConditionPredicate.EQ, new JsonTerm("testName"))),
              Variable.of(
                  BuiltCondition.of("age", BuiltConditionPredicate.EQ, new JsonTerm("testAge"))));
      Query query = new QueryBuilder().select().from("ks", "tbl").where(expression).build();
      assertThat(query.cql()).isEqualTo("SELECT * FROM ks.tbl WHERE (\"Name\" = ? AND age = ?)");
      assertThat(query.values()).contains("testName", "testAge");
    }

    @Test
    public void vsearch() {
      Expression<BuiltCondition> expression = null;
      Query query =
          new QueryBuilder()
              .select()
              .from("ks", "tbl")
              .vsearch(VECTOR_COLUMN, TEST_VECTOR)
              .where(expression)
              .build();
      assertThat(query.cql())
          .isEqualTo("SELECT * FROM ks.tbl ORDER BY query_vector_value ANN OF ?");
      assertThat(query.values()).contains(TEST_CQL_VECTOR);
    }

    @Test
    public void vsearchWithFilter() {
      Expression<BuiltCondition> expression =
          ExpressionUtils.andOf(
              Variable.of(
                  BuiltCondition.of("name", BuiltConditionPredicate.EQ, new JsonTerm("testName"))));
      Query query =
          new QueryBuilder()
              .select()
              .column("a", "b")
              .from("ks", "tbl")
              .similarityFunction(
                  VECTOR_COLUMN, CollectionSchemaObject.SimilarityFunction.EUCLIDEAN)
              .vsearch(VECTOR_COLUMN, TEST_VECTOR)
              .where(expression)
              .limit(10)
              .build();
      assertThat(query.cql())
          .isEqualTo(
              "SELECT a, b, SIMILARITY_EUCLIDEAN(query_vector_value, ?) FROM ks.tbl WHERE name = ? ORDER BY query_vector_value ANN OF ? LIMIT 10");
      assertThat(query.values()).isEqualTo(List.of(TEST_CQL_VECTOR, "testName", TEST_CQL_VECTOR));
    }

    @Test
    public void simpleOr() {
      Expression<BuiltCondition> expression =
          ExpressionUtils.orOf(
              Variable.of(
                  BuiltCondition.of("name", BuiltConditionPredicate.EQ, new JsonTerm("testName"))),
              Variable.of(
                  BuiltCondition.of("age", BuiltConditionPredicate.EQ, new JsonTerm("testAge"))));
      Query query = new QueryBuilder().select().from("ks", "tbl").where(expression).build();
      assertThat(query.cql()).isEqualTo("SELECT * FROM ks.tbl WHERE (name = ? OR age = ?)");
      assertThat(query.values()).contains("testName", "testAge");
    }

    @Test
    public void singleVariableWithoutParenthesis() {
      Expression<BuiltCondition> expression1 =
          ExpressionUtils.andOf(
              Variable.of(
                  BuiltCondition.of("name", BuiltConditionPredicate.EQ, new JsonTerm("testName"))));
      Query query1 = new QueryBuilder().select().from("ks", "tbl").where(expression1).build();
      assertThat(query1.cql()).isEqualTo("SELECT * FROM ks.tbl WHERE name = ?");
      assertThat(query1.values()).containsOnly("testName");
    }

    @Test
    public void nestedAndOr() {
      Expression<BuiltCondition> expression2 =
          ExpressionUtils.orOf(
              Variable.of(
                  BuiltCondition.of(
                      "address", BuiltConditionPredicate.EQ, new JsonTerm("testAddress"))),
              ExpressionUtils.andOf(
                  Variable.of(
                      BuiltCondition.of(
                          "name", BuiltConditionPredicate.EQ, new JsonTerm("testName"))),
                  Variable.of(
                      BuiltCondition.of(
                          "age", BuiltConditionPredicate.EQ, new JsonTerm("testAge")))));
      Query query2 = new QueryBuilder().select().from("ks", "tbl").where(expression2).build();
      assertThat(query2.cql())
          .isEqualTo("SELECT * FROM ks.tbl WHERE (address = ? OR (name = ? AND age = ?))");
      assertThat(query2.values()).contains("testName", "testAge", "testAddress");
    }

    @Test
    public void singleVariableExpression() {
      Expression<BuiltCondition> expression2 =
          ExpressionUtils.orOf(
              Variable.of(
                  BuiltCondition.of(
                      "address", BuiltConditionPredicate.EQ, new JsonTerm("testAddress"))),
              ExpressionUtils.andOf(
                  Variable.of(
                      BuiltCondition.of(
                          "age", BuiltConditionPredicate.EQ, new JsonTerm("testAge")))));
      Query query2 = new QueryBuilder().select().from("ks", "tbl").where(expression2).build();
      assertThat(query2.cql()).isEqualTo("SELECT * FROM ks.tbl WHERE (address = ? OR age = ?)");
      assertThat(query2.values()).contains("testAge", "testAddress");
    }
  }
}
