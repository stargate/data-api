package io.stargate.sgv2.jsonapi.testbench.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.TestBenchPlan;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

/**
 * Describes functions that can be used to create instances of an assertion, and finds the factory
 */
public sealed interface AssertionFactory {

  /**
   * Registry of the factories that can be called to create assertions, see {@link
   * AssertionFactoryRegistry}
   */
  AssertionFactoryRegistry REGISTRY = new AssertionFactoryRegistry();

  /**
   * A factory that returns a single AssertionMatcher, this is a single match of the response.
   *
   * <p>This is what we use with basic assertions like:
   *
   * <pre>
   *     {
   *        "Status.isExactly" : {
   *           "matchedCount": 1,
   *           "modifiedCount": 1
   *         }
   *     }
   * </pre>
   */
  @FunctionalInterface
  non-sealed interface AssertionMatcherFactory extends AssertionFactory {
    /**
     * Create an assertion matcher that can be used to match the response.
     *
     * @param testCommand The command the assertion will be run against.
     * @param args The arguments defined in the test suite, e.g. the number of documents in a
     *     collection.
     * @return AssertionMatcher that can be used to match the response.
     */
    AssertionMatcher create(TestCommand testCommand, JsonNode args);
  }

  /**
   * A factory that returns a list of assertions, this is a list of matches of the response. Used
   * with templated assertions.
   *
   * <p>This returns the {@link TestAssertion} which is higher up the stack than the {@link
   * AssertionMatcher} because a templated assertion is a list of assertions and only the template
   * factory knows how to describe them because it makes them.
   */
  @FunctionalInterface
  non-sealed interface TemplatedAssertionFactory extends AssertionFactory {

    /**
     * Create a list of assertions that can be used to match the response.
     *
     * @param testPlan The test plan that is being created holds context of what we are doing.
     * @param template JSON pulled from the {@link
     *     io.stargate.sgv2.jsonapi.testbench.testspec.TestSpecKind#ASSERTION_TEMPLATE} that matched
     *     the assertion name.
     * @param testCommand The command the assertion will be run against.
     * @param args The arguments defined in the test suite, e.g. the number of documents in a
     *     collection.
     * @return
     */
    List<TestAssertion> create(
        TestBenchPlan testPlan, JsonNode template, TestCommand testCommand, JsonNode args);
  }

  /** Returns true if the function matches *either* of the assertion factory functions. */
  static boolean isValidFactoryMethod(Method method) {

    // must be static
    if (!Modifier.isStatic(method.getModifiers())) {
      return false;
    }

    // Checking for TemplatedAssertionFactory
    // NOTE: not checked the generic type of the list, lazy and this is a contrained world
    if (List.class.isAssignableFrom(method.getReturnType())) {
      var p = method.getParameterTypes();
      return p.length == 4
          && p[0] == TestBenchPlan.class
          && p[1] == JsonNode.class
          && p[2] == TestCommand.class
          && p[3] == JsonNode.class;
    }

    // Checking for AssertionMatcherFactory
    if (method.getReturnType() == AssertionMatcher.class) {
      var p = method.getParameterTypes();
      return p.length == 2 && p[0] == TestCommand.class && p[1] == JsonNode.class;
    }
    return false;
  }

  /**
   * There are two situations we handle the factory methods with: first when registering them so we
   * know what we can call, second when processing a test suite config we need to call them to get
   * the assertion.
   *
   * <p>For the first part, the factory methods are defined as static functions on a class, and
   * there can be two different types of factory. This class wraps the raw factory, so we have a
   * common class we can use when registering all the factories we know. It also constructs a {@link
   * AssertionName} for the java function that can be used to match against the name used in the
   * test suite config.
   *
   * <p>For the second path the subclasses provide an adapter functionality: they implement the
   * factory interface so we get strong type checking for calling, and then transform that call into
   * an untyped call to the raw factory method via {@link Method#invoke(Object, Object...)}.
   *
   * <p><b>NOTE:</b> Construct instances using {@link WrappedMethod#of(Class, Method)}
   */
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

      // a little sloppy, but we already know this should be a factory method
      return (method.getReturnType() == AssertionMatcher.class)
          ? new WrappedAssertionMatcherFactory(clazz, method)
          : new WrappedTemplatedAssertionFactory(clazz, method);
    }

    /** Gets the name of the factory function, without the class name. */
    public String properName() {
      return AssertionName.properName(method);
    }

    /**
     * The identifier for the factory function that can be used to match agains the name used in the
     * test suite config. e.g. "Documents.count"
     */
    public AssertionName assertionName() {
      return assertionName;
    }

    @SuppressWarnings("unchecked")
    protected <T> T invoke(Object... args) {
      try {
        // pass null for the object reference, all factories are static
        return (T) method.invoke(null, args);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Adapter class for the {@link AssertionMatcherFactory}, translates strong typed called to the
   * factory method into untypes method invocation.
   */
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

  /**
   * Adapter class for the {@link TemplatedAssertionFactory}, translates strong typed called to the
   * factory method into untypes method invocation.
   */
  final class WrappedTemplatedAssertionFactory extends WrappedMethod
      implements TemplatedAssertionFactory {

    WrappedTemplatedAssertionFactory(Class<?> clazz, Method method) {
      super(clazz, method);
    }

    @Override
    public List<TestAssertion> create(
        TestBenchPlan testPlan, JsonNode template, TestCommand testCommand, JsonNode args) {
      return invoke(testPlan, template, testCommand, args);
    }
  }
}
