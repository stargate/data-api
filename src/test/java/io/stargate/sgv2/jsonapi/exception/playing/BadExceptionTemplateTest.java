package io.stargate.sgv2.jsonapi.exception.playing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test creating the new API Exceptions using YAML templating in the file {@link
 * ErrorTestData#TEST_ERROR_CONFIG_FILE}
 */
public class BadExceptionTemplateTest {

  @BeforeAll
  public static void setup() throws IOException {
    ErrorConfig.unsafeInitializeFromYamlResource(ErrorTestData.MISSING_CTOR_ERROR_CONFIG_FILE);
  }

  @AfterAll
  public static void teardown() throws IOException {
    ErrorConfig.unsafeInitializeFromYamlResource(ErrorConfig.DEFAULT_ERROR_CONFIG_FILE);
  }

  @Test
  public void missingCtorFails() {

    /// This is tricky, because the exception code is designed to be easy and work. So if our test
    // references the ErrorCode enum for a bad exception it will fail but fail as the ENUM is being
    // constructed which we cannot catch.
    // So need to make the template manually as a workaround, this is what the error code enum would
    // do
    // but we CANNOT cause the Code enum to be constructed as it will fail.

    // the error ID must match the one in the config file, from setUP
    var error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ErrorTemplate.load(
                    MissingCtorException.class,
                    MissingCtorException.FAMILY,
                    MissingCtorException.SCOPE,
                    "EXCEPTION_MISSING_CTOR"),
            "Trying to make a template for an exception that is missing the required CTOR should fail");

    assertThat(error)
        .message()
        .startsWith(
            "Failed to find constructor that accepts APIException.ErrorInstance.class for the exception class");
  }

  @Test
  public void failingCtorFails() {

    var error =
        assertThrows(
            IllegalStateException.class,
            FailingCtorScopeException.Code.EXCEPTION_FAILING_CTOR::get,
            "Trying to make a template for an exception that fails in the constructor should fail");

    assertThat(error).message().startsWith("Failed to create a new instance of");

    var reflectionError = error.getCause();
    assertThat(reflectionError)
        .as("Error from template wraps the InvocationTargetException from reflection")
        .isInstanceOf(InvocationTargetException.class);

    var rootCause = reflectionError.getCause();
    assertThat(rootCause)
        .as("Root cause is the exception thrown in the constructor")
        .isInstanceOf(RuntimeException.class)
        .hasMessage("BANG");
  }
}
