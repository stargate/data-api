package io.stargate.sgv2.jsonapi.exception.playing;

/**
 * Re-usable test data for tests on the exceptions, so we dont need to use inheritance where it may
 * not make sense.
 */
public class ErrorTestData {

  public static final String TEST_ERROR_CONFIG_FILE = "test_errors.yaml";

  public static final String MISSING_CTOR_ERROR_CONFIG_FILE = "invalid_exception_errors.yaml";

  // Variables for templates
  public final String VAR_NAME = "name-" + System.currentTimeMillis();
  public final String VAR_VALUE = "value-" + System.currentTimeMillis();

  // Just a random string to use when needed
  public final String RANDOM_STRING = "random-" + System.currentTimeMillis();
}
