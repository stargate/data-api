package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.LEXICAL_CONTENT_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorizeSortExpression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link SortExpression#from(SortExpression2)}: each kind of {@link SortExpression2}
 * becomes a sort expression with the same path and values, and no values of the other kinds.
 */
class SortExpressionTest {

  @Test
  void fromVectorizeSortHasTheText() {
    var expression = SortExpression.from(new VectorizeSortExpression("cheese"));

    assertThat(expression.getPath()).isEqualTo(VECTOR_EMBEDDING_TEXT_FIELD);
    assertThat(expression.getVectorize()).isEqualTo("cheese");
    assertThat(expression.hasVector()).isFalse();
    assertThat(expression.getLexicalQuery()).isNull();
  }

  @Test
  void fromVectorSortHasTheVector() {
    var expression = SortExpression.from(new VectorSortExpression(new float[] {0.1f, 0.2f}));

    assertThat(expression.getPath()).isEqualTo(VECTOR_EMBEDDING_FIELD);
    assertThat(expression.getVector()).containsExactly(0.1f, 0.2f);
    assertThat(expression.hasVectorize()).isFalse();
    assertThat(expression.getLexicalQuery()).isNull();
  }

  @Test
  void fromLexicalSortHasTheText() {
    var expression = SortExpression.from(new LexicalSortExpression("cows"));

    assertThat(expression.getPath()).isEqualTo(LEXICAL_CONTENT_FIELD);
    assertThat(expression.getLexicalQuery()).isEqualTo("cows");
    assertThat(expression.isLexicalSort()).isTrue();
    assertThat(expression.hasVector()).isFalse();
    assertThat(expression.hasVectorize()).isFalse();
  }

  @ParameterizedTest(name = "ascending={0}")
  @ValueSource(booleans = {true, false})
  void fromFieldSortHasThePathAndTheOrder(boolean ascending) {
    var expression = SortExpression.from(new FieldSortExpression("address.city", ascending));

    assertThat(expression.getPath()).isEqualTo("address.city");
    assertThat(expression.isAscending()).isEqualTo(ascending);
    assertThat(expression.hasVector()).isFalse();
    assertThat(expression.hasVectorize()).isFalse();
    assertThat(expression.getLexicalQuery()).isNull();
  }

  @Test
  void fromRejectsNull() {
    assertThatThrownBy(() -> SortExpression.from(null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("expression must not be null");
  }
}
