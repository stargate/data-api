package io.stargate.sgv2.jsonapi.testbench.targets;

import io.stargate.sgv2.jsonapi.testbench.lifecycle.JobLifeCycle;
import io.stargate.sgv2.jsonapi.testbench.lifecycle.TestPlanLifecycle;

import java.util.regex.Pattern;

/**
 * A class of backend database the tests will be run against. This is not a partcular insance of a DB to run
 * against, for that see {@link Target}.
 * <p>
 * There is some different behavior between C* and Astra, and potentially in any future versions of
 * those products.
 * </p>
 */
public abstract class Backend implements TestPlanLifecycle, JobLifeCycle {

  private static final Pattern PATTERN_NOT_WORD_CHARS = Pattern.compile("\\W+");

  /**
   * Sanitizes a schema name to be valid in all C* derived platforms.
   */
  public static String toSafeSchemaIdentifier(String name) {

    var newValue = PATTERN_NOT_WORD_CHARS.matcher(name).replaceAll("_");
    if (newValue.length() > 48) {
      return newValue.substring(0, 47);
    }
    return newValue;
  }
}
