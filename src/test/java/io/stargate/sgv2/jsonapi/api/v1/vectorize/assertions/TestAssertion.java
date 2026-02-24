package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCase;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestResponse;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public record TestAssertion(
    String name,
    JsonNode args,
    AssertionMatcher matcher
) {

  public void run(TestResponse testResponse) {

    try{
      matcher.match(testResponse.apiResponse());
    }
    catch (AssertionError e) {
      System.out.printf("Failed Assertion: name=%s, args=%s", name, args);
      throw e;
    }
    catch (Exception e) {
      System.out.printf("Error In Assertion: name=%s, args=%s", name, args);
      throw e;
    }
  }
  public static List<TestAssertion> forSuccess(CommandName commandName) {

    var builder = Stream.<AssertionDefinition>builder()
        .add(new AssertionDefinition("Statuscode.success", null));

    switch (commandName) {
      case INSERT_ONE, INSERT_MANY -> {
        builder.add(new AssertionDefinition("Response.isWriteSuccess", null));
      }
      case CREATE_KEYSPACE, DROP_KEYSPACE, CREATE_NAMESPACE, DROP_NAMESPACE, DELETE_COLLECTION, CREATE_COLLECTION -> {
        builder.add(new AssertionDefinition("Response.isDDLSuccess", null));
      }
    }
    return buildAssertions(builder.build());
  }


  public static List<TestAssertion> buildAssertions(TestCase testCase) {

    return buildAssertions(testCase.asserts().properties().stream()
        .map(AssertionDefinition::create));
  }

  private static List<TestAssertion> buildAssertions(Stream<AssertionDefinition> defs) {

    return defs.map(
        def -> {
          var assertFactory = findAssertionFactory(def.name());
          AssertionMatcher matcher;
          try {
            matcher = (AssertionMatcher) assertFactory.invoke(null, def.args());
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
          return new TestAssertion(def.name(), def.args(), matcher);
        }
    ).toList();

  }

  private static Method findAssertionFactory(String key) {
    // "validatableResponse.isFindSuccess"

    int dot = key.indexOf('.');
    String typeName = key.substring(0, dot);
    String funcName = key.substring(dot + 1);

    String qualifiedTypeName =
        "io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions."
            + Character.toUpperCase(typeName.charAt(0))
            + typeName.substring(1).toLowerCase();

    try {
      Class<?> cls = Class.forName(qualifiedTypeName);

      var factoryMethod = Arrays.stream(cls.getMethods())
          .filter(m -> m.getName().equalsIgnoreCase(funcName))
          .filter(m -> Modifier.isStatic(m.getModifiers()))
          .findFirst()
          .orElseThrow();


      return factoryMethod;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Invalid assertion: " + key, e);
    }
  }

  private record AssertionDefinition(String name, JsonNode args) {

    static AssertionDefinition create(Map.Entry<String, JsonNode> def) {
      return new AssertionDefinition(def.getKey(), def.getValue());
    }

  }

}
