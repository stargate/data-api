package io.stargate.sgv2.jsonapi.testbench.assertions;

import java.lang.reflect.Method;

/**
 * A normalized way to represent an assertion name, used in both directions: used when registering
 * assertion factories, so we map from the Java method name to this; and then to parse the assertion
 * name from the test spec the user provided.
 *
 * <p>Once in this common middle ground, we can map between test spec and java method names.
 * <b>NOTE:</b> the matching is case-insensitive, and we normalize to lower case. So "isSuccess" and
 * "issuccess" are the same.
 *
 * @param typeName Class name of the method, or the first part of the assertion config e.g.
 *     "Documents"
 * @param funcName The name of the method, or the second part of the assertion config e.g. "count"
 */
public record AssertionName(String typeName, String funcName) {

  // A bit of a hack, buit this is used when we force the static load. see below.
  private static final String PACKAGE = "io.stargate.sgv2.jsonapi.testbench.assertions";

  public AssertionName {
    // we normalize to match on case-insensitive
    typeName = typeName.toLowerCase();
    funcName = funcName.toLowerCase();
  }

  /** Create from the string name provided in the test spec config, e.g. "Documents.count" */
  public static AssertionName from(String fullKey) {

    int pos = fullKey.indexOf('.');
    if (pos < 0) {
      throw new IllegalArgumentException("fullKey must have a dot: " + fullKey);
    }
    var type = fullKey.substring(0, pos);
    var func = fullKey.substring(pos + 1);
    return new AssertionName(type, func);
  }

  /** Gets the name of the factory function, without the class name. */
  public static String properName(Method method) {
    return method.getName();
  }

  public String properClassName() {
    return PACKAGE + "." + Character.toUpperCase(typeName.charAt(0)) + typeName.substring(1);
  }
}
