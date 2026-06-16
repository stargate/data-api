package io.stargate.sgv2.jsonapi.util.asserts;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.SchemaException;

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
