package io.stargate.sgv2.jsonapi.util.asserts;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.SchemaException;

/**
 * Central place to export all new assertions added for the Data API, e.g. same as {@link
 * org.assertj.core.api.Assertions}.
 *
 * <p>Assertion helpers in this package should write package-protected factories in their class, and
 * then add them here as public.
 */
public class DataAPIAsserts {

  private DataAPIAsserts() {}

  public static CommandResultAssert assertThatCommandResult(CommandResult commandResult) {
    return CommandResultAssert.assertThatCommandResult(commandResult);
  }

  public static SchemaExceptionAssert assertThatSchemaException(Throwable throwable) {
    return SchemaExceptionAssert.assertThatSchemaException(throwable);
  }

  public static SchemaExceptionAssert assertThatSchemaException(SchemaException schemaException) {
    return SchemaExceptionAssert.assertThatSchemaException(schemaException);
  }
}
