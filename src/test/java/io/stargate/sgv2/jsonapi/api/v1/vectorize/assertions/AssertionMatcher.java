package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.APIResponse;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;
import org.assertj.core.api.AssertFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Contract for running an assertion on the response from the API.
 */
@FunctionalInterface
public interface AssertionMatcher {

  /**
   * Match the response to the assertion.
   *
   * @param apiResponse response from the API
   * @throws AssertionError if the match fails
   */
  void match(APIResponse apiResponse);

  AssertionFactoryRegistry FACTORY_REGISTRY = new AssertionFactoryRegistry();

  /**
   * Assertions int the test case are used to find Assertion factories in the code,
   * they are called with the TestCommand they need to run against so you
   * know what the command is, and any args from the JSON for the test case.
   * <p>
   * Use the {@link SingleFactory} signature if your factory returns a single matcher, which is
   * normal. Use the {@link MultiFactory} signature if you want to return more than one.
   * </p>
   */

  interface AssertionFactory<T> extends BiFunction<TestCommand, JsonNode, T> {}

  interface AssertionMatcherFactory extends AssertionFactory<AssertionMatcher> {
  }

  interface TestAssertionContainerFactory extends AssertionFactory<List<TestAssertion>> {}

  class AssertionFactoryRegistry {

    private final Map<String, AssertionFactory<?>> factoryMethods = new ConcurrentHashMap<>();

    public void register(Class<?> cls) {
      for (var method : cls.getMethods()) {
        if (isValidFactoryMethod(method)) {
          var factoryKey = cls.getSimpleName().toLowerCase()
              + "."
              + method.getName().toLowerCase();

          // NOTE: not checking the generic of the list
          AssertionFactory<?> wrapped = (method.getReturnType() == AssertionMatcher.class) ?
              new AssertionMatcherFactoryWrapper(method)
              :
              new TestAssertionContainerFactoryWrapper(method);
          factoryMethods.put(factoryKey, wrapped);
        }
      }
    }

    public AssertionFactory<?> get(String name){
      var normalName = name.toLowerCase();

      int pos = normalName.indexOf('.');
      if (pos < 0){
        throw new IllegalArgumentException("Name must have a dot: " + name);
      }
      var type = normalName.substring(0, pos);
      var func = normalName.substring(pos + 1);

      if (type.equals( "template")) {
        return TemplatedAssertions.getFactory(func);
      }

      var factoryMethod = factoryMethods.get(normalName);
      if (factoryMethod == null) {
        loadClassFor(name);
      }

      factoryMethod = factoryMethods.get(normalName);
      if (factoryMethod == null) {
        throw new IllegalArgumentException("Unknown assertion factory. (normalised)name: %s known=%s".formatted(name,factoryMethods.keySet()));
      }
      return factoryMethod;
    }

    private static final String PACKAGE =
        "io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions";

    private void loadClassFor(String key) {

      int dot = key.indexOf('.');
      if (dot < 0) {
        return;
      }

      var typeName = key.substring(0, dot);

      var className =
          PACKAGE + "."
              + Character.toUpperCase(typeName.charAt(0))
              + typeName.substring(1);

      try {
        Class.forName(className);

        // class static initializer should call register()

      } catch (ClassNotFoundException ignored) {
        // expected if class does not exist
      }
    }

    private record AssertionMatcherFactoryWrapper(Method method) implements AssertionMatcherFactory {

      @SuppressWarnings("unchecked")
      @Override
      public AssertionMatcher apply(TestCommand testCommand, JsonNode args) {
        try {
          return (AssertionMatcher) method.invoke(null, testCommand, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private record TestAssertionContainerFactoryWrapper(Method method) implements TestAssertionContainerFactory {

      @SuppressWarnings("unchecked")
      @Override
      public List<TestAssertion> apply(TestCommand testCommand, JsonNode args) {
        try {
          return (List<TestAssertion>) method.invoke(null, testCommand, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private static boolean isValidFactoryMethod(Method method) {

      if (!Modifier.isStatic(method.getModifiers())) {
        return false;
      }

      if ((!List.class.isAssignableFrom(method.getReturnType())) && method.getReturnType() != AssertionMatcher.class) {
        return false;
      }
      var p = method.getParameterTypes();
      return p.length == 2
          && p[0] == TestCommand.class
          && p[1] == JsonNode.class;
    }

  }
}
