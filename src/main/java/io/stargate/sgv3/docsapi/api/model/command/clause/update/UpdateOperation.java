package io.stargate.sgv3.docsapi.api.model.command.clause.update;

/**
 * UpdateOperation represents a unit of data to be updated. A update clause can have list of
 * UpdateOperation.
 *
 * @param path - Json path to be updated
 * @param operator
 * @param value
 * @param <T>
 */
public record UpdateOperation<T>(String path, UpdateOperator operator, UpdateValue<T> value) {
  /**
   * Represents the value to be updated
   *
   * @param value
   * @param valueType
   * @param <T>
   */
  public record UpdateValue<T>(T value, ValueType valueType) {
    /** Types of data accepted for updates */
    public enum ValueType {
      BOOLEAN,
      NUMBER,
      STRING,
      NULL,
      SUB_DOC,
      ARRAY
    }
  }
}
