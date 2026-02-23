package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.APIResponse;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCase;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface TestAssertion {

  void run(APIResponse apiResponse);

  static List<TestAssertion> forSuccess(CommandName commandName) {

    List<TestAssertion> assertions = new ArrayList<>();
    assertions.add(StatusCode.success(null));

    switch (commandName) {
      case INSERT_ONE, INSERT_MANY -> {
        assertions.add(Response.isWriteSuccess(null));
      }
      case CREATE_KEYSPACE, DROP_KEYSPACE, CREATE_NAMESPACE, DROP_NAMESPACE, DELETE_COLLECTION, CREATE_COLLECTION -> {
        assertions.add(Response.isDDLSuccess(null));
      }
    }
    return assertions;
  }


  public static List<TestAssertion> buildAssertions (TestCase testCase) {

    List<TestAssertion> testAssertions = new ArrayList<>();
    for (Map.Entry<String, JsonNode> entry : testCase.asserts().properties()){
      var args = entry.getValue();

      var assertFactory = findAssertionFactory(entry.getKey());
      try {
        testAssertions.add((BodyAssertion)assertFactory.invoke(null, args));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
    return testAssertions;
  }

  private static Method findAssertionFactory(String key){
    // "validatableResponse.isFindSuccess"

    int dot = key.indexOf('.');
    String typeName = key.substring(0, dot);
    String funcName   = key.substring(dot + 1);

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

}
