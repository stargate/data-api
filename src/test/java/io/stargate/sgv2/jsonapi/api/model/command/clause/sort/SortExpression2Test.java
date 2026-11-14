package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorizeSortExpression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for the {@link SortExpression2} records. */
class SortExpression2Test {

  @Test
  void vectorSortComparesVectorByValue() {
    var vectorSort = new VectorSortExpression(new float[] {1.1f, 2.2f, 3.3f});
    var sameVector = new VectorSortExpression(new float[] {1.1f, 2.2f, 3.3f});
    var diffVector = new VectorSortExpression(new float[] {1.1f, 2.2f, 4.4f});

    assertThat(vectorSort).as("same vector").isEqualTo(sameVector);
    assertThat(vectorSort.hashCode()).as("same vector hash").isEqualTo(sameVector.hashCode());
    assertThat(vectorSort).as("different vector").isNotEqualTo(diffVector);
  }

  @Test
  void pathIsTheSortField() {
    assertThat(new VectorizeSortExpression("cheese").path()).isEqualTo("$vectorize");
    assertThat(new VectorSortExpression(new float[] {1.1f}).path()).isEqualTo("$vector");
    assertThat(new LexicalSortExpression("cows").path()).isEqualTo("$lexical");
    assertThat(new FieldSortExpression("address.city", true).path()).isEqualTo("address.city");
  }

  /** The deserializer rejects blank text with an API error, so blank text here is a bug. */
  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void textSortsRejectBlankText(String text) {
    assertThrowsExactly(IllegalArgumentException.class, () -> new VectorizeSortExpression(text));
    assertThrowsExactly(IllegalArgumentException.class, () -> new LexicalSortExpression(text));
  }

  @Test
  void fieldSortRejectsEmptyPath() {
    assertThrowsExactly(IllegalArgumentException.class, () -> new FieldSortExpression("", true));
    // a blank name is a valid field name, see FieldNamingRule
    assertThat(new FieldSortExpression(" ", true).path()).isEqualTo(" ");
  }
}
