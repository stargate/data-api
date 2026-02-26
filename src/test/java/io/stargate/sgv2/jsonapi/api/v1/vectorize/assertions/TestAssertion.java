package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCase;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestResponse;
import org.junit.jupiter.api.DynamicNode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public interface TestAssertion {

  String name();

  JsonNode args();

  void run(TestResponse testResponse);

  DynamicNode testNodes(AtomicReference<TestResponse> testResponse);


  static List<TestAssertion> forSuccess(TestCommand testCommand) {

    var builder = Stream.<AssertionDefinition>builder()
        .add(new AssertionDefinition("template.isSuccess", null));

    return buildAssertions(testCommand, builder.build());
  }

  static List<TestAssertion> buildAssertions(TestCase testCase) {

    var defs = testCase.asserts().properties().stream()
        .map(AssertionDefinition::create);
    return buildAssertions(testCase.command(), defs);
  }

  static List<TestAssertion> buildAssertions(TestCommand testCommand, List<AssertionDefinition> defs) {

    return buildAssertions(testCommand, defs.stream());
  }

  static List<TestAssertion> buildAssertions(TestCommand testCommand, Stream<AssertionDefinition> defs) {

    return defs.map(
        def -> buildAssertion(testCommand, def)
    ).toList();
  }

  public static TestAssertion buildAssertion(TestCommand testCommand, AssertionDefinition def) {

    return switch (AssertionMatcher.FACTORY_REGISTRY.get(def.name())) {
      case AssertionMatcher.AssertionMatcherFactory factory ->
          new SingleTestAssertion(def.name(), def.args(), factory.apply(testCommand, def.args()));
      case AssertionMatcher.TestAssertionContainerFactory factory ->
          new TestAssertionContainer(def.name(), def.args(), factory.apply(testCommand, def.args));
      default -> throw new IllegalStateException("Unknown TestAssertionFactory: " + def.name());
    };
  }

  record AssertionDefinition(String name, JsonNode args) {

    static AssertionDefinition create(Map.Entry<String, JsonNode> def) {
      return new AssertionDefinition(def.getKey(), def.getValue());
    }

  }

}
