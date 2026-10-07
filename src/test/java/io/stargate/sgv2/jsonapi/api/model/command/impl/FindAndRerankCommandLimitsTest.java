package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests that reranking limits retain integer values without truncation or overflow. */
class FindAndRerankCommandLimitsTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @ParameterizedTest
  @MethodSource("invalidNumericLimits")
  void rejectsInvalidNumericLimits(String options, String path, String value) {
    var error =
        assertThrows(
            JsonMappingException.class,
            () -> objectMapper.readValue(commandWithOptions(options), FindAndRerankCommand.class));

    assertThat(error)
        .hasMessageContaining(path)
        .hasMessageContaining("must be an integer")
        .hasMessageContaining(value);
  }

  private static Stream<Arguments> invalidNumericLimits() {
    return Stream.of(
            "0.5",
            "5.9",
            "50.9",
            "3000000000",
            "-3000000000",
            "4294967306",
            "-4294967286",
            "18446744073709551626")
        .flatMap(
            value ->
                Stream.of(
                    Arguments.of("{\"limit\": %s}".formatted(value), "options.limit", value),
                    Arguments.of(
                        "{\"hybridLimits\": %s}".formatted(value), "options.hybridLimits", value),
                    Arguments.of(
                        "{\"hybridLimits\": {\"$vector\": %s, \"$lexical\": 10}}".formatted(value),
                        "options.hybridLimits.$vector",
                        value),
                    Arguments.of(
                        "{\"hybridLimits\": {\"$vector\": 10, \"$lexical\": %s}}".formatted(value),
                        "options.hybridLimits.$lexical",
                        value)));
  }

  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 1, 50, Integer.MAX_VALUE})
  void preservesIntegerValuesForLaterRangeValidation(int value) throws Exception {
    var command =
        objectMapper.readValue(
            commandWithOptions("{\"limit\": %d, \"hybridLimits\": %d}".formatted(value, value)),
            FindAndRerankCommand.class);

    assertThat(command.options().limit()).isEqualTo(value);
    assertThat(command.options().hybridLimits().vectorLimit()).isEqualTo(value);
    assertThat(command.options().hybridLimits().lexicalLimit()).isEqualTo(value);

    command =
        objectMapper.readValue(
            commandWithOptions(
                "{\"hybridLimits\": {\"$vector\": %d, \"$lexical\": %d}}".formatted(value, value)),
            FindAndRerankCommand.class);

    assertThat(command.options().hybridLimits().vectorLimit()).isEqualTo(value);
    assertThat(command.options().hybridLimits().lexicalLimit()).isEqualTo(value);
  }

  @ParameterizedTest
  @ValueSource(strings = {"{}", "{\"limit\": null, \"hybridLimits\": null}"})
  void preservesDefaultLimits(String options) throws Exception {
    var command = objectMapper.readValue(commandWithOptions(options), FindAndRerankCommand.class);

    assertThat(command.options().limit()).isNull();
    assertThat(command.options().hybridLimits()).isNull();
  }

  private static String commandWithOptions(String options) {
    return "{\"findAndRerank\": {\"options\": %s}}".formatted(options);
  }
}
