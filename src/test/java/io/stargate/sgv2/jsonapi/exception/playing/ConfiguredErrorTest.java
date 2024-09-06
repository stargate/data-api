package io.stargate.sgv2.jsonapi.exception.playing;

import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Base for tests that use one of the test resource files for configuring the {@link ErrorConfig}
 * and {@link ErrorTemplate} classes.
 *
 * <p>Using <code>@TestInstance(TestInstance.Lifecycle.PER_CLASS)</code> allows the <code>@BeforeAll
 * </code> to be instance method rather than static,
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class ConfiguredErrorTest {

  /**
   * Tells the base class which error config file to use for the tests.
   *
   * <p>
   *
   * @return Name of the error file to configure the error system with, typically something defined
   *     on {@link ErrorTestData}.
   */
  abstract String getErrorConfigResource();

  @BeforeAll
  public void setup() throws IOException {
    ErrorConfig.unsafeInitializeFromYamlResource(getErrorConfigResource());
  }

  @AfterAll
  public void teardown() throws IOException {
    ErrorConfig.unsafeInitializeFromYamlResource(ErrorConfig.DEFAULT_ERROR_CONFIG_FILE);
  }
}
