package io.stargate.sgv2.jsonapi.service.processor;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import java.util.Objects;

/**
 * Interface for any class that can be validated against a {@link SchemaObject}.
 *
 * <p>Implementations <b>must</b> cascade validation to other objects in their state if they are
 * also validatable. This is to ensure that the entire object graph is validated. If an object is
 * not valid it should throw an exception, normally a {@link
 * io.stargate.sgv2.jsonapi.exception.playing.APIException}. Implement only the <code>validate*
 * </code> functions that are relevant to the object, the default behaviour will be the throw a
 * {@link UnsupportedOperationException} when not implemented. This is desirable as it will detect
 * situations where code is going down a path using a type of schema object we do not expect.
 *
 * <p>Implementations <b>should</b> add doc comments on the methods they implement to say what they
 * are validating. This interface should make it easier / clearer to find where the Data API
 * validates requests and what it checks.
 *
 * <p>Code that wants to validate an object should call {@link #maybeValidate(CommandContext,
 * SchemaValidatable)} with the object to validate. This will then call the appropriate validate
 * method
 *
 * <p>Example:
 *
 * <pre>
 *  SchemaValidatable.maybeValidate(commandContext, command.filterClause());
 * </pre>
 */
public interface SchemaValidatable {

  /**
   * Calls the supplied validatable object to validate against the {@link SchemaObject} from the
   * {@link CommandContext} using one of the dedicated <code>validate*</code> methods on the
   * interface.
   *
   * <p>NOTE: Code that wants to validate an object should call this static method with the context
   * rather than the instance methods directly.
   *
   * @param commandContext The context the command is running against, including the {@link
   *     SchemaObject}
   * @param validatable An object that implements {@link SchemaValidatable}, may be null
   * @param <T> Type of the {@link SchemaObject}
   */
  static <T extends SchemaObject> void maybeValidate(
      CommandContext<T> commandContext, SchemaValidatable validatable) {

    if (validatable == null) {
      return;
    }

    Objects.requireNonNull(commandContext, "commandContext must not be null");
    switch (commandContext.schemaObject()) {
      case CollectionSchemaObject ignored ->
          validatable.validateCollection(commandContext.asCollectionContext());
      case TableSchemaObject ignored -> validatable.validateTable(commandContext.asTableContext());
      case KeyspaceSchemaObject ignored ->
          validatable.validateKeyspace(commandContext.asKeyspaceContext());
      case DatabaseSchemaObject ignored ->
          validatable.validateDatabase(commandContext.asDatabaseContext());
      default ->
          throw new UnsupportedOperationException(
              String.format("Unsupported schema type: %s", commandContext.schemaObject().type()));
    }
  }

  /**
   * Implementations should implement this method if they support validation against a {@link
   * CollectionSchemaObject}.
   *
   * <p>Only implement this method if the objects supports Collections, the default implementation
   * is to fail.
   *
   * @param commandContext {@link CommandContext<CollectionSchemaObject>} to validate against
   */
  default void validateCollection(CommandContext<CollectionSchemaObject> commandContext) {
    throw new UnsupportedOperationException(unsupportedMessage(commandContext.schemaObject()));
  }

  /**
   * Implementations should implement this method if they support validation against a {@link
   * TableSchemaObject}.
   *
   * <p>Only implement this method if the object supports Tables, the default implementation is to
   * fail.
   *
   * @param commandContext {@link CommandContext<TableSchemaObject>} to validate against
   */
  default void validateTable(CommandContext<TableSchemaObject> commandContext) {
    throw new UnsupportedOperationException(unsupportedMessage(commandContext.schemaObject()));
  }

  /**
   * Implementations should implement this method if they support validation against a {@link
   * KeyspaceSchemaObject}.
   *
   * <p>Only implement this method if the object supports Keyspace, the default implementation is to
   * fail.
   *
   * @param commandContext {@link CommandContext<KeyspaceSchemaObject>} to validate against
   */
  default void validateKeyspace(CommandContext<KeyspaceSchemaObject> commandContext) {
    throw new UnsupportedOperationException(unsupportedMessage(commandContext.schemaObject()));
  }

  /**
   * Implementations should implement this method if they support validation against a {@link
   * DatabaseSchemaObject}.
   *
   * <p>Only implement this method if the object supports Databases, the default implementation is
   * to fail.
   *
   * @param commandContext {@link CommandContext<DatabaseSchemaObject>} to validate against
   */
  default void validateDatabase(CommandContext<DatabaseSchemaObject> commandContext) {
    throw new UnsupportedOperationException(unsupportedMessage(commandContext.schemaObject()));
  }

  /** Helper to return the string used in the exception message when schema type not supported. */
  default String unsupportedMessage(SchemaObject schemaObject) {
    return String.format(
        "%s object does not support validating against schema type %s, target name: %s",
        getClass().getSimpleName(), schemaObject.type(), schemaObject.name());
  }
}
