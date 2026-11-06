package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.*;
import static io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class FindAndRerankSortTest {
  @Test
  void hybridObjectComparesVectorValues() {
    var first = new HybridObject("cheese", "cows", new float[] {1, 2, 3}, CommandFeatures.EMPTY);
    var equal = new HybridObject("cheese", "cows", new float[] {1, 2, 3}, CommandFeatures.EMPTY);
    var different =
        new HybridObject("cheese", "cows", new float[] {1, 2, 4}, CommandFeatures.EMPTY);

    assertThat(first).isEqualTo(equal).isNotEqualTo(different);
    assertThat(first.hashCode()).isEqualTo(equal.hashCode());
  }

  @Test
  void vectorExpressionComparesVectorValues() {
    var first = new VectorSe(new float[] {1, 2, 3});
    var equal = new VectorSe(new float[] {1, 2, 3});
    var different = new VectorSe(new float[] {1, 2, 4});

    assertThat(first).isEqualTo(equal).isNotEqualTo(different);
    assertThat(first.hashCode()).isEqualTo(equal.hashCode());
  }

  @Test
  void hybridVectorDoesNotChangeWhenArraysAreModified() {
    var vector = new float[] {1, 2, 3};
    var sort = new HybridObject(null, "cows", vector, CommandFeatures.EMPTY);
    vector[0] = 4;
    sort.vector()[1] = 5;

    assertThat(sort.vector()).containsExactly(1, 2, 3);
  }

  @Test
  void vectorExpressionDoesNotChangeWhenArraysAreModified() {
    var vector = new float[] {1, 2, 3};
    var expression = new VectorSe(vector);
    vector[0] = 4;
    expression.vector()[1] = 5;

    assertThat(expression.vector()).containsExactly(1, 2, 3);
  }

  @Test
  void lexicalLegRequiresOneLexicalExpression() {
    assertThrows(
        IllegalArgumentException.class, () -> new LexicalLeg(List.of(), CommandFeatures.EMPTY));
    assertThrows(
        IllegalArgumentException.class,
        () -> new LexicalLeg(List.of(new FieldSe("age", true)), CommandFeatures.EMPTY));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LexicalLeg(
                List.of(new LexicalSe("cows"), new LexicalSe("cheese")), CommandFeatures.EMPTY));
  }

  @Test
  void vectorLegRequiresOneVectorOrVectorizeExpression() {
    assertThrows(
        IllegalArgumentException.class, () -> new VectorLeg(List.of(), CommandFeatures.EMPTY));
    assertThrows(
        IllegalArgumentException.class,
        () -> new VectorLeg(List.of(new LexicalSe("cows")), CommandFeatures.EMPTY));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new VectorLeg(
                List.of(new VectorSe(new float[] {1}), new VectorizeSe("cheese")),
                CommandFeatures.EMPTY));
  }

  @Test
  void filterLegRejectsSearchExpressions() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new FilterLeg(
                List.of(new FieldSe("age", true), new LexicalSe("cows")), CommandFeatures.EMPTY));
    assertThat(new FilterLeg(List.of(), CommandFeatures.EMPTY).sortExpressions()).isEmpty();
  }

  @Test
  void legOwnsItsExpressionList() {
    var expressions = new ArrayList<SortExpression2>();
    expressions.add(new FieldSe("age", true));
    var leg = new FilterLeg(expressions, CommandFeatures.EMPTY);
    expressions.add(new FieldSe("name", false));

    assertThat(leg.sortExpressions()).containsExactly(new FieldSe("age", true));
    assertThrows(
        UnsupportedOperationException.class,
        () -> leg.sortExpressions().add(new FieldSe("name", false)));
  }
}
