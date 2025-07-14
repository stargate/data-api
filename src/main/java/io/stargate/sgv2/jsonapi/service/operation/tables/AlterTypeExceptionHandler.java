package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AlterTypeExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier udtName;
  private final List<String> allFieldRenames;
  private final List<String> allAddFieldNames;

  /**
   * Constructor for AlterTypeExceptionHandler.
   *
   * @param schemaObject
   * @param statement
   * @param udtName Name of the UDT that is being altered.
   * @param allFieldRenames List of the field names that are being renamed, the from names not the
   *     to names.
   */
  public AlterTypeExceptionHandler(
      KeyspaceSchemaObject schemaObject,
      SimpleStatement statement,
      CqlIdentifier udtName,
      List<String> allFieldRenames,
      List<String> allAddFieldNames) {
    super(schemaObject, statement);

    this.udtName = Objects.requireNonNull(udtName, "udtName must not be null");
    this.allFieldRenames = allFieldRenames == null ? List.of() : List.copyOf(allFieldRenames);
    this.allAddFieldNames = allAddFieldNames == null ? List.of() : List.copyOf(allAddFieldNames);
  }

  @Override
  public RuntimeException handle(InvalidQueryException exception) {

    // Note, "Unkown" is a typo in driver, and the message returns the keyspace name when it
    // it should be the type name. e.g. below is an error on the "demo.address" type:
    // "Unkown field fake in user type demo"
    if (exception.getMessage().contains("Unkown field")) {
      return SchemaException.Code.CANNOT_RENAME_UNKNOWN_TYPE_FIELD.get(
          "typeName",
          errFmt(udtName),
          "unknownField",
          getFieldName(exception.getMessage(), "Unkown field ", " in user type", allFieldRenames));
    }

    // Message for name collision for add is
    // "Cannot add field city to type demo.address: a field with name city already exists""
    if (exception.getMessage().contains("a field with name")) {
      return SchemaException.Code.CANNOT_ADD_EXISTING_FIELD.get(
          Map.of(
              "typeName",
              errFmt(udtName),
              "existingField",
              getFieldName(
                  exception.getMessage(),
                  "a field with name",
                  "already exists",
                  allAddFieldNames)));
    }

    // Message for a name collision for rename is, NOTE has typos and the wrong keyspace name:
    // demo is the keyspace name, not the type name.
    ///  "Duplicate field name street in type demo"
    // Re-using the CANNOT_ADD_EXISTING_FIELD code because it is roughly the same thing.
    if (exception.getMessage().contains("Duplicate field name")) {
      return SchemaException.Code.CANNOT_ADD_EXISTING_FIELD.get(
          Map.of(
              "typeName",
              errFmt(udtName),
              "existingField",
              getFieldName(
                  exception.getMessage(), "Duplicate field name", "in type", allAddFieldNames)));
    }

    return super.handle(exception);
  }

  private String getFieldName(
      String message, String prefix, String suffix, List<String> fallbacks) {

    // attempt to get the field name from the message
    int startIndex = message.indexOf(prefix);
    int endIndex = message.indexOf(suffix, startIndex);

    if (startIndex > -1 && endIndex > startIndex) {
      return message.substring(startIndex + prefix.length(), endIndex).trim();
    }

    return "unable to determine field name, is one of: " + errFmtJoin(fallbacks);
  }
}
