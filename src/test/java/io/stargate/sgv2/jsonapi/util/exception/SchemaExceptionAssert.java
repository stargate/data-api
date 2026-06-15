package io.stargate.sgv2.jsonapi.util.exception;

import io.stargate.sgv2.jsonapi.exception.SchemaException;

public class SchemaExceptionAssert
    extends APIExceptionAssert<SchemaExceptionAssert, SchemaException> {

  private SchemaExceptionAssert(SchemaException actual) {
    super(actual, SchemaExceptionAssert.class);
  }

  public static SchemaExceptionAssert assertThatSchemaException(SchemaException schemaException) {
    return assertThatAPIException(
        SchemaExceptionAssert::new, SchemaException.class, schemaException);
  }

  public static SchemaExceptionAssert assertThatSchemaException(Throwable throwable) {
    return assertThatAPIException(SchemaExceptionAssert::new, SchemaException.class, throwable);
  }
}
