package io.stargate.sgv2.jsonapi.testbench.assertions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of assertion factories that is built by the static constructors of the <b>assertion
 * classes registering themselves.</bold>
 *
 * <p>Every assertion class must register itself with the registry, so we know what factories exist
 * and can match them at test execution time. Do that in a static constructor as below, the regsitry
 * will walk the class and registry functions that match either of the two {@link AssertionFactory}
 * interfaces.
 *
 * <pre>
 * public class Documents {
 *
 *   static {
 *     AssertionFactory.REGISTRY.register(Documents.class);
 *   }
 * </pre>
 *
 * <p>Use the singleton instance at {@link AssertionFactory#REGISTRY} to access the registry.
 */
public class AssertionFactoryRegistry {

  // Map of the factory functions and the name they should be matched to in test suite config
  private final Map<AssertionName, AssertionFactory.WrappedMethod> factoryMethods =
      new ConcurrentHashMap<>();

  /**
   * Registers all the static factory functions on the class that match the signature of either
   * {@link AssertionFactory} interface.
   */
  public void register(Class<?> cls) {

    for (var method : cls.getMethods()) {
      if (AssertionFactory.isValidFactoryMethod(method)) {
        var wrapped = AssertionFactory.WrappedMethod.of(cls, method);
        factoryMethods.put(wrapped.assertionName(), wrapped);
      }
    }
  }

  /**
   * Gets the factory function for the assertion with the given name.
   *
   * @param rawAssertionName the string name of the assertion from the config, e.g.
   *     "Documents.count"
   * @return A wrapper for the factory function that can be used to create an assertion instance.
   */
  public AssertionFactory.WrappedMethod getWrappedAssertionFactory(String rawAssertionName) {

    // need to take the raw name from the config into the internal representation that
    // matches to what we used in the registry
    var assertionName = AssertionName.from(rawAssertionName);

    var factoryMethod = factoryMethods.get(assertionName);
    if (factoryMethod == null) {
      // sometimes the class is not loaded yet, so let's just give nature a helping hand. Shhh
      loadClassFor(assertionName);
    }

    factoryMethod = factoryMethods.get(assertionName);
    if (factoryMethod == null) {
      throw new IllegalArgumentException(
          "Unknown assertion factory. (parsed) assertionName: %s factoryMethods.keySet:%s"
              .formatted(assertionName, factoryMethods.keySet()));
    }
    return factoryMethod;
  }

  private void loadClassFor(AssertionName normalisedName) {

    try {
      Class.forName(normalisedName.properClassName());
      // class static initializer should call register()
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Unknown assertion factory. normalisedName=%s, properClassName()=%s"
              .formatted(normalisedName, normalisedName.properClassName()),
          e);
    }
  }
}
