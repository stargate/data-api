package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCase;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestPlan;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestResponse;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.AssertionTemplateSpec;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.SpecFiles;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestUri;
import org.assertj.core.api.AssertFactory;
import org.junit.jupiter.api.DynamicNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public interface TestAssertion {

  String name();

  JsonNode args();

  void run(TestResponse testResponse);

  DynamicNode testNodes(TestUri.Builder uriBuilder, AtomicReference<TestResponse> testResponse);


  static List<TestAssertion> forSuccess(TestPlan testPlan, TestCommand testCommand) {

    var builder = Stream.<AssertionDefinition>builder()
        .add(new AssertionDefinition("Templated.isSuccess", null));

    return buildAssertions(testPlan, testCommand, builder.build());
  }

  static List<TestAssertion> buildAssertions(TestPlan testPlan, TestCase testCase) {

    var defs = testCase.asserts().properties().stream()
        .map(AssertionDefinition::create);
    return buildAssertions(testPlan, testCase.command(), defs);
  }

  static List<TestAssertion> buildAssertions(TestPlan testPlan, TestCommand testCommand, Stream<AssertionDefinition> defs) {

    return defs.map(
        def -> buildAssertion(testPlan, testCommand, def)
    ).toList();
  }

  static TestAssertion buildAssertion(TestPlan testPlan, TestCommand testCommand, AssertionDefinition def) {
    return def.addFactory(AssertionFactory.REGISTRY).build(testPlan, testCommand);
  }

  /**
   * <p> </p>
   */
  record AssertionDefinition(String name, JsonNode args) {

    static AssertionDefinition create(Map.Entry<String, JsonNode> def) {
      return new AssertionDefinition(def.getKey(), def.getValue());
    }

    AssertionDefWithFactory addFactory(AssertionFactoryRegistry registry) {

      var factory = registry.getWrapped(name());
      if (factory == null) {
        throw new  IllegalStateException("Unknown Assertion Factory name=" + name());
      }
      return new AssertionDefWithFactory(factory, args);
    }
  }

  /**
   * <p> </p>
   */
  record AssertionDefWithFactory(AssertionFactory.WrappedMethod method, JsonNode args){

    TestAssertion build(TestPlan testPlan, TestCommand testCommand) {

      return switch (method) {
        case AssertionFactory.WrappedAssertionMatcherFactory factory ->
            new SingleTestAssertion(method.properName(), args(), factory.create(testCommand, args()));

        case AssertionFactory.TemplatedAssertionFactory factory ->{

            var template = testPlan.specFiles().byType(AssertionTemplateSpec.class)
                .flatMap(assertTemplate -> assertTemplate.templateFor(method.properName()).stream())
                .findFirst()
                .orElseThrow(() -> new  IllegalStateException("Unknown Assertion Template name=" + method.properName()));

            yield new TestAssertionContainer(method.properName(), args(), factory.create(testPlan, template, testCommand, args()));
        }
      };
    }

  }
}
