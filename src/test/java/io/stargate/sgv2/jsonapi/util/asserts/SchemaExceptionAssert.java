package io.stargate.sgv2.jsonapi.util.asserts;

import io.stargate.sgv2.jsonapi.exception.SchemaException;

/** See {@link APIExceptionAssert} and {@link DataAPIAsserts} */
public class SchemaExceptionAssert
    extends APIExceptionAssert<SchemaExceptionAssert, SchemaException> {

  private SchemaExceptionAssert(SchemaException actual) {
    super(actual, SchemaExceptionAssert.class);
  }

  protected static SchemaExceptionAssert assertThatSchemaException(
      SchemaException schemaException) {

    return assertThatAPIException(
        SchemaExceptionAssert::new, SchemaException.class, schemaException);
  }

  protected static SchemaExceptionAssert assertThatSchemaException(Throwable throwable) {

    return assertThatAPIException(SchemaExceptionAssert::new, SchemaException.class, throwable);
  }
}
