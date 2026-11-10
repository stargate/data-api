package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.metrics.CommandFeature.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link FindAndRerankSortDefinition}: Jackson keeps any sort as sent, and the metrics
 * features come from the field names.
 */
class FindAndRerankSortDefinitionTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Reading the command does not check the sort, so these sorts are kept as sent. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(
      strings = {
        "{\"$hybrid\": \"cheese\"}",
        "{\"$hybrid\": 1}",
        "{\"$hybrid\": {\"fake\": 1}}",
        "{\"age\": 2}",
        "{}",
        "\"cheese\"",
        "1",
        "[1, 2]"
      })
  void keepsAnySortAsSent(String sort) throws Exception {
    var command =
        MAPPER.readValue(
            "{\"findAndRerank\": {\"sort\": " + sort + "}}", FindAndRerankCommand.class);

    assertThat(command.sortClause().json()).isEqualTo(MAPPER.readTree(sort));
  }

  /** A command with no sort or a null sort gets {@link FindAndRerankSortDefinition#NO_ARG_SORT}. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"{\"findAndRerank\": {}}", "{\"findAndRerank\": {\"sort\": null}}"})
  void usesNoArgSortWhenThereIsNoSort(String json) throws Exception {
    var command = MAPPER.readValue(json, FindAndRerankCommand.class);

    assertThat(command.sortClause()).isSameAs(FindAndRerankSortDefinition.NO_ARG_SORT);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("commandFeaturesCases")
  void recordsCommandFeaturesFromFieldNames(String sort, CommandFeatures expected)
      throws Exception {
    var definition = new FindAndRerankSortDefinition(MAPPER.readTree(sort));

    assertThat(definition.commandFeatures()).isEqualTo(expected);
  }

  static Stream<Arguments> commandFeaturesCases() {
    return Stream.of(
        // ----
        // $hybrid
        Arguments.of(
            """
            {"$hybrid": "cheese"}
            """,
            CommandFeatures.of(HYBRID)),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows", "$vector": [1.1, 2.2]}}
            """,
            CommandFeatures.of(HYBRID, VECTORIZE, LEXICAL, VECTOR)),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": null}}
            """,
            CommandFeatures.of(HYBRID, VECTORIZE, LEXICAL)),
        Arguments.of(
            """
            {"$hybrid": {"$lexical": "cows"}}
            """,
            CommandFeatures.of(HYBRID, LEXICAL)),
        Arguments.of(
            """
            {"$hybrid": 1}
            """,
            CommandFeatures.of(HYBRID)),
        // ----
        // single leg sorts
        Arguments.of(
            """
            {"$vectorize": "cheese"}
            """,
            CommandFeatures.of(VECTORIZE)),
        Arguments.of(
            """
            {"$vector": [1.1, 2.2]}
            """,
            CommandFeatures.of(VECTOR)),
        Arguments.of(
            """
            {"$lexical": null}
            """,
            CommandFeatures.of(LEXICAL)),
        Arguments.of(
            """
            {"$vector": [1.1, 2.2], "age": 1}
            """,
            CommandFeatures.of(VECTOR)),
        Arguments.of(
            """
            {"age": -1, "name": 1}
            """,
            CommandFeatures.EMPTY),
        Arguments.of(
            """
            {}
            """,
            CommandFeatures.EMPTY),
        Arguments.of(
            """
            "cheese"
            """,
            CommandFeatures.EMPTY));
  }

  @Test
  void noArgSortHasNoCommandFeatures() {
    assertThat(FindAndRerankSortDefinition.NO_ARG_SORT.commandFeatures())
        .isEqualTo(CommandFeatures.EMPTY);
  }
}
