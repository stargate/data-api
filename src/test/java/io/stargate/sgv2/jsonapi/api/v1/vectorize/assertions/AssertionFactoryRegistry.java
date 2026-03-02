package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AssertionFactoryRegistry {

  private final Map<AssertionName, AssertionFactory.WrappedMethod> factoryMethods = new ConcurrentHashMap<>();


  public void register(Class<?> cls) {
    for (var method : cls.getMethods()) {
      if ( AssertionFactory.isValidFactoryMethod(method)) {
        var wrapped = AssertionFactory.WrappedMethod.of(cls, method);
        factoryMethods.put(wrapped.assertionName(), wrapped);
      }
    }
  }

  public AssertionFactory.WrappedMethod getWrapped(String fullKey) {
    var normalisedName = AssertionName.from(fullKey);

    var factoryMethod = factoryMethods.get(normalisedName);
    if (factoryMethod == null) {
      loadClassFor(normalisedName);
    }

    factoryMethod = factoryMethods.get(normalisedName);
    if (factoryMethod == null) {
      throw new IllegalArgumentException("Unknown assertion factory. (normalised)name: %s known=%s".formatted(normalisedName, factoryMethods.keySet()));
    }
    return factoryMethod;
  }

  private void loadClassFor(AssertionName normalisedName) {

    try {
      Class.forName(normalisedName.properClassName());
      // class static initializer should call register()
    } catch (ClassNotFoundException e) {
      throw  new IllegalArgumentException("Unknown assertion factory. normalisedName=%s, properClassName()=%s".formatted(normalisedName, normalisedName.properClassName()), e);
    }
  }

}
