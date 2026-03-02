package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestPlan;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

/**

 */

public sealed interface AssertionFactory {

  public static final AssertionFactoryRegistry REGISTRY = new AssertionFactoryRegistry();

  @FunctionalInterface
  non-sealed interface AssertionMatcherFactory extends AssertionFactory {
    AssertionMatcher create(TestCommand testCommand, JsonNode args);
  }

  @FunctionalInterface
  non-sealed interface TemplatedAssertionFactory extends AssertionFactory {
    List<TestAssertion> create(TestPlan testPlan, JsonNode template, TestCommand testCommand, JsonNode args);
  }

  static boolean isValidFactoryMethod(Method method) {

    if (!Modifier.isStatic(method.getModifiers())) {
      return false;
    }

    // NOTE: not checked the type of the list, lazy
    if (List.class.isAssignableFrom(method.getReturnType())) {
      var p = method.getParameterTypes();
      return p.length == 4
          && p[0] == TestPlan.class
          && p[1] == JsonNode.class
          && p[2] == TestCommand.class
          && p[3] == JsonNode.class;
    }

    if (method.getReturnType() == AssertionMatcher.class) {
      var p = method.getParameterTypes();
      return p.length == 2
          && p[0] == TestCommand.class
          && p[1] == JsonNode.class;
    }
    return false;
  }

  abstract sealed class WrappedMethod
      permits WrappedAssertionMatcherFactory, WrappedTemplatedAssertionFactory {

    private final Class<?> clazz;
    private final Method method;
    private final AssertionName assertionName;

    protected WrappedMethod(Class<?> clazz, Method method) {
      this.clazz = clazz;
      this.method = method;
      this.assertionName = new AssertionName(clazz.getSimpleName(), method.getName());
    }

    static WrappedMethod of(Class<?> clazz, Method method) {
      return  (method.getReturnType() == AssertionMatcher.class) ?
          new WrappedAssertionMatcherFactory(clazz, method)
          :
          new WrappedTemplatedAssertionFactory(clazz, method);
    }

    public Class<?> clazz() {
      return clazz;
    }

    public Method method() {
      return method;
    }

    public String properName() {
      return AssertionName.properName(method);
    }

    public AssertionName assertionName() {
      return assertionName;
    }

    @SuppressWarnings("unchecked")
    protected <T> T invoke(Object... args) {
      try {
        return (T) method.invoke(null, args);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }

  final class WrappedAssertionMatcherFactory extends WrappedMethod
      implements AssertionMatcherFactory {

    WrappedAssertionMatcherFactory(Class<?> clazz, Method method) {
      super(clazz, method);
    }

    @Override
    public AssertionMatcher create(TestCommand testCommand, JsonNode args) {
      return invoke(testCommand, args);
    }
  }

  final class WrappedTemplatedAssertionFactory extends WrappedMethod
      implements TemplatedAssertionFactory {

    WrappedTemplatedAssertionFactory(Class<?> clazz, Method method) {
      super(clazz, method);
    }

    @Override
    public List<TestAssertion> create(TestPlan testPlan, JsonNode template, TestCommand testCommand, JsonNode args) {
      return invoke(testPlan, template, testCommand, args);
    }
  }
}
