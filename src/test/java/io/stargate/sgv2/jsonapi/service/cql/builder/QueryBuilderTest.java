package io.stargate.sgv2.jsonapi.service.cql.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Variable;
import com.datastax.oss.driver.api.core.data.CqlVector;
import io.stargate.sgv2.jsonapi.service.cql.ExpressionUtils;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.JsonTerm;
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

  @ParameterizedTest
  @MethodSource("sampleQueries")
  @DisplayName("Should generate expected CQL string")
  public void generateCql(String actualCql, String expectedCql) {
    assertThat(actualCql).isEqualTo(expectedCql);
  }

  @SuppressWarnings("PMD.ExcessiveMethodLength")
  public static Arguments[] sampleQueries() {
    return new Arguments[] {
      arguments(
          new QueryBuilder().select().from("ks", "tbl").build().cql(), "SELECT * FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().column("a", "b", "c").from("ks", "tbl").build().cql(),
          "SELECT a, b, c FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").build().cql(),
          "SELECT COUNT(a) FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().count().from("ks", "tbl").build().cql(),
          "SELECT COUNT(1) FROM ks.tbl"),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").limit(1).build().cql(),
          "SELECT COUNT(a) FROM ks.tbl LIMIT 1"),
      arguments(
          new QueryBuilder().select().count("a").from("ks", "tbl").limit().build().cql(),
          "SELECT COUNT(a) FROM ks.tbl LIMIT ?"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .from("ks", "tbl")
              .limit(1)
              .vsearch(VECTOR_COLUMN, TEST_VECTOR)
              .build()
              .cql(),
          "SELECT a, b, c FROM ks.tbl ORDER BY query_vector_value ANN OF ? LIMIT 1"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .similarityFunction(
                  "query_vector_value", CollectionSettings.SimilarityFunction.COSINE)
              .from("ks", "tbl")
              .limit(1)
              .vsearch(VECTOR_COLUMN, TEST_VECTOR)
              .build()
              .cql(),
          "SELECT a, b, c, SIMILARITY_COSINE(query_vector_value, ?) FROM ks.tbl ORDER BY query_vector_value ANN OF ? LIMIT 1"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .similarityFunction(
                  "query_vector_value", CollectionSettings.SimilarityFunction.DOT_PRODUCT)
              .from("ks", "tbl")
              .limit(1)
              .vsearch(VECTOR_COLUMN, TEST_VECTOR)
              .build()
              .cql(),
          "SELECT a, b, c, SIMILARITY_DOT_PRODUCT(query_vector_value, ?) FROM ks.tbl ORDER BY query_vector_value ANN OF ? LIMIT 1"),
      arguments(
          new QueryBuilder()
              .select()
              .column("a", "b", "c")
              .similarityFunction(VECTOR_COLUMN, CollectionSettings.SimilarityFunction.EUCLIDEAN)
              .from("ks", "tbl")
              .limit(1)
              .vsearch("query_vector_value", TEST_VECTOR)
              .build()
              .cql(),
          "SELECT a, b, c, SIMILARITY_EUCLIDEAN(query_vector_value, ?) FROM ks.tbl ORDER BY query_vector_value ANN OF ? LIMIT 1"),
    };
  }

  @Nested
  public class expressionToCqlBuilderTest {
    @Test
    public void simpleAnd() {
      Expression<BuiltCondition> expression =
          ExpressionUtils.andOf(
              Variable.of(BuiltCondition.of("name", Predicate.EQ, new JsonTerm("testName"))),
              Variable.of(BuiltCondition.of("age", Predicate.EQ, new JsonTerm("testAge"))));
      Query query = new QueryBuilder().select().from("ks", "tbl").where(expression).build();
      assertThat(query.cql()).isEqualTo("SELECT * FROM ks.tbl WHERE (name = ? AND age = ?)");
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
    public void simpleOr() {
      Expression<BuiltCondition> expression =
          ExpressionUtils.orOf(
              Variable.of(BuiltCondition.of("name", Predicate.EQ, new JsonTerm("testName"))),
              Variable.of(BuiltCondition.of("age", Predicate.EQ, new JsonTerm("testAge"))));
      Query query = new QueryBuilder().select().from("ks", "tbl").where(expression).build();
      assertThat(query.cql()).isEqualTo("SELECT * FROM ks.tbl WHERE (name = ? OR age = ?)");
      assertThat(query.values()).contains("testName", "testAge");
    }

    @Test
    public void singleVariableWithoutParenthesis() {
      Expression<BuiltCondition> expression1 =
          ExpressionUtils.andOf(
              Variable.of(BuiltCondition.of("name", Predicate.EQ, new JsonTerm("testName"))));
      Query query1 = new QueryBuilder().select().from("ks", "tbl").where(expression1).build();
      assertThat(query1.cql()).isEqualTo("SELECT * FROM ks.tbl WHERE name = ?");
      assertThat(query1.values()).containsOnly("testName");
    }

    @Test
    public void nestedAndOr() {
      Expression<BuiltCondition> expression2 =
          ExpressionUtils.orOf(
              Variable.of(BuiltCondition.of("address", Predicate.EQ, new JsonTerm("testAddress"))),
              ExpressionUtils.andOf(
                  Variable.of(BuiltCondition.of("name", Predicate.EQ, new JsonTerm("testName"))),
                  Variable.of(BuiltCondition.of("age", Predicate.EQ, new JsonTerm("testAge")))));
      Query query2 = new QueryBuilder().select().from("ks", "tbl").where(expression2).build();
      assertThat(query2.cql())
          .isEqualTo("SELECT * FROM ks.tbl WHERE (address = ? OR (name = ? AND age = ?))");
      assertThat(query2.values()).contains("testName", "testAge", "testAddress");
    }

    @Test
    public void singleVariableExpression() {
      Expression<BuiltCondition> expression2 =
          ExpressionUtils.orOf(
              Variable.of(BuiltCondition.of("address", Predicate.EQ, new JsonTerm("testAddress"))),
              ExpressionUtils.andOf(
                  Variable.of(BuiltCondition.of("age", Predicate.EQ, new JsonTerm("testAge")))));
      Query query2 = new QueryBuilder().select().from("ks", "tbl").where(expression2).build();
      assertThat(query2.cql()).isEqualTo("SELECT * FROM ks.tbl WHERE (address = ? OR age = ?)");
      assertThat(query2.values()).contains("testAge", "testAddress");
    }
  }
}
