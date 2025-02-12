package io.stargate.sgv2.jsonapi.service.schema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.service.schema.naming.SchemaObjectNamingRule;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Unit tests for the {@link NamingRules} class. */
public class NamingRulesTests {

  /** Tests the naming rules for schema objects like Keyspace, Collection, Table, and Index. */
  @ParameterizedTest
  @MethodSource("schemaObjectNamingTestCases")
  public void schemaObjectNamingValidation(
      String invalidName,
      SchemaObjectNamingRule namingRule,
      boolean expectedResult,
      String description) {

    assertThat(namingRule.apply(invalidName)).as(description).isEqualTo(expectedResult);
  }

  private static Stream<Arguments> schemaObjectNamingTestCases() {
    // Define a simple record to encapsulate a test case.
    record TestCase(String name, boolean expected, String description) {}

    // List of cases
    List<TestCase> generalInvalidCases =
        Arrays.asList(
            new TestCase(null, false, "name cannot be null"),
            new TestCase("", false, "name cannot be empty"),
            new TestCase(" ", false, "name cannot be blank"),
            new TestCase("a b", false, "name cannot contain spaces"),
            new TestCase(
                "this_is_a_string_that_is_designed_to_be_more_than_one_hundred_characters_long_so_we_can_demonstrate_a_valid_example_with_extra_text_here",
                false,
                "name cannot be longer than 100 characters"),
            new TestCase(
                "!@#$%^&*()=-[]{}';:.></?`~", false, "name cannot contain special characters"),
            new TestCase("a", true, "name can be 1 character"),
            new TestCase("123", true, "name can be numbers only"),
            new TestCase("a1", true, "name can be alphanumeric"),
            new TestCase("ABC", true, "name can be uppercase"),
            new TestCase("_a", true, "name can start with underscore"),
            new TestCase("Ab12A_", true, "name can be mixed case"));

    // List of naming rules to test.
    List<SchemaObjectNamingRule> namingRules =
        Arrays.asList(
            NamingRules.KEYSPACE, NamingRules.COLLECTION, NamingRules.TABLE, NamingRules.INDEX);

    // Combine each naming rule with each test case.
    var generalTestCases =
        namingRules.stream()
            .flatMap(
                rule ->
                    generalInvalidCases.stream()
                        .map(
                            tc ->
                                Arguments.of(
                                    tc.name,
                                    rule,
                                    tc.expected,
                                    rule.schemaType().apiName() + tc.description)));

    // Create the stream of special test cases.
    Stream<Arguments> specialTestCases =
        Stream.of(
            Arguments.of(
                "this_is_a_very_long_name_that_is_longer_than_48_characters",
                NamingRules.INDEX,
                true,
                "Index name can be longer than 48 characters"));

    return Stream.concat(generalTestCases, specialTestCases);
  }
}
