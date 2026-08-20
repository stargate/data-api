package io.stargate.sgv2.jsonapi.testbench.assertions;

/**
 * Functional interface to call so an assertion can describe itself outside of its string
 * representation. Typically based on how the assertion was described in the test configuration.
 */
@FunctionalInterface
public interface Describable {

  String describe();
}
