package io.stargate.sgv2.jsonapi.service.processor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for the {@link SchemaValidatable} */
public class SchemaValidatableTest {

  private final SchemaObjectTestData SCHEMA_TEST_DATA = new SchemaObjectTestData();
  private final CommandContextTestData COMMAND_CONTEXT_TEST_DATA = new CommandContextTestData();

  @Test
  public void nullVerifiable() {
    // command context is not used if verifiable is null
    assertDoesNotThrow(
        () -> SchemaValidatable.maybeValidate(SCHEMA_TEST_DATA.MOCK_COLLECTION, null));
  }

  @Test
  public void nullCommandContext() {

    var e =
        assertThrows(
            NullPointerException.class,
            () ->
                SchemaValidatable.maybeValidate(
                    (CommandContext<CollectionSchemaObject>) null, mockValidatable()));

    assertThat(e).hasMessage("commandContext must not be null");
  }

  @Test
  public void nullSchemaObjectContext() {

    var e =
        assertThrows(
            NullPointerException.class,
            () ->
                SchemaValidatable.maybeValidate((CollectionSchemaObject) null, mockValidatable()));

    assertThat(e).hasMessage("schemaObject must not be null");
  }

  @ParameterizedTest
  @MethodSource("schemaTypeTestCases")
  public <T extends SchemaObject> void throwWhenNotImplemented(Class<T> schemeTypeClass) {

    var noImplementations = new SchemaValidatable() {};
    var schemaObject = SCHEMA_TEST_DATA.prebuiltMock(schemeTypeClass);
    var context = COMMAND_CONTEXT_TEST_DATA.mockCommandContext(schemaObject);

    var e =
        assertThrowsExactly(
            UnsupportedOperationException.class,
            () -> SchemaValidatable.maybeValidate(context, noImplementations),
            "Expected exception when schema object type validation not implemented");

    assertThat(e)
        .message()
        .contains(
            "object does not support validating against schema type " + schemaObject.type().name())
        .contains(" target name: " + schemaObject.name().toString());
  }

  @ParameterizedTest
  @MethodSource("schemaTypeTestCases")
  public <T extends SchemaObject> void callCorrectValidate(Class<T> schemeTypeClass) {

    var validatable = mockValidatable();
    var schemaObject = SCHEMA_TEST_DATA.prebuiltMock(schemeTypeClass);
    var context = COMMAND_CONTEXT_TEST_DATA.mockCommandContext(schemaObject);

    assertDoesNotThrow(
        () -> SchemaValidatable.maybeValidate(context, validatable),
        "No exception thrown when the validatable implements the correct method");

    // Verify that method1 was called exactly once
    var verifying = verify(validatable, times(1));
    switch (schemaObject) {
      case DatabaseSchemaObject database -> verifying.validate(database);
      case KeyspaceSchemaObject keyspace -> verifying.validate(keyspace);
      case CollectionSchemaObject collection -> verifying.validate(collection);
      case TableSchemaObject table -> verifying.validate(table);
      default -> fail("Unknown schema object class: " + schemaObject.getClass());
    }

    // Verify that no other methods were called
    verifyNoMoreInteractions(validatable);
  }

  private static Stream<Arguments> schemaTypeTestCases() {
    return Stream.of(
        Arguments.of(DatabaseSchemaObject.class),
        Arguments.of(KeyspaceSchemaObject.class),
        Arguments.of(CollectionSchemaObject.class),
        Arguments.of(TableSchemaObject.class));
  }

  private SchemaValidatable mockValidatable() {
    return mock(SchemaValidatable.class);
  }
}
