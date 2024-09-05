package io.stargate.sgv2.jsonapi.exception;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Static helper functions to consistently format objects into strings when building error messages.
 *
 * <p>This is a utility class and should not be instantiated, do static function imports to make it
 * easier to use.
 *
 * <p>Probably want to use the {@link #errFmt(SchemaObject, Consumer)} normally, and then call the
 * others from the see consumer.
 */
public abstract class ErrorFormatters {

  public static final String DELIMITER = ", ";

  public static <T> String join(Collection<T> list, Function<T, String> formatter) {
    if (list.isEmpty()) {
      return "[None]";
    }
    return list.stream().map(formatter).collect(Collectors.joining(DELIMITER));
  }

  public static String errFmtColumnMetadata(Collection<ColumnMetadata> columns) {
    return join(columns, ErrorFormatters::errFmt);
  }

  public static String errFmtCqlIdentifier(Collection<CqlIdentifier> identifiers) {
    return join(identifiers, ErrorFormatters::errFmt);
  }

  public static String errFmt(ColumnMetadata column) {
    return String.format("%s(%s)", errFmt(column.getName()), errFmt(column.getType()));
  }

  public static String errFmt(CqlIdentifier identifier) {
    return identifier.asCql(true);
  }

  public static String errFmt(DataType dataType) {
    // TODO:  should this return the API Table name for the type?
    return dataType.asCql(true, true);
  }

  public static Map<String, String> errFmt(SchemaObject schemaObject) {
    return errFmt(schemaObject, null);
  }

  /**
   * Adds variables to a map for the <code>schemaObject</code> and then calls the consumer to add
   * more.
   *
   * <p>This makes it easy to get basic schema variables into the map and then add more as needed.
   * Remember, we can have more variables in the maps for the template that the template uses, but
   * not the other way around. Example:
   *
   * <pre>
   *     public RuntimeException handle(TableSchemaObject schemaObject, WriteTimeoutException exception) {
   *     return DatabaseException.Code.TABLE_WRITE_TIMEOUT.get(
   *         errFmt(
   *             schemaObject,
   *             m -> {
   *               m.put("blockFor", String.valueOf(exception.getBlockFor()));
   *               m.put("received", String.valueOf(exception.getReceived()));
   *             }));
   *   }
   * </pre>
   *
   * @param schemaObject The schema object to get the basic variables from, variables are added for
   *     <code>schemaType</code>, <code>keyspace</code>, and <code>table</code>.
   * @param consumer The consumer to add more variables to the map.
   * @return Map with the basic schema object variables and any additional variables added by the
   *     consumer.
   */
  public static Map<String, String> errFmt(
      SchemaObject schemaObject, Consumer<Map<String, String>> consumer) {
    Map<String, String> map = new HashMap<>();
    map.put("schemaType", schemaObject.type.name());
    map.put("keyspace", schemaObject.name.keyspace());
    map.put("table", schemaObject.name.table());
    if (consumer != null) {
      consumer.accept(map);
    }
    return map;
  }

  public static Map<String, String> errFmt(Throwable runtimeException) {
    Map<String, String> map = new HashMap<>();
    map.put("errorClass", runtimeException.getClass().getSimpleName());
    map.put("errorMessage", runtimeException.getMessage());
    return map;
  }
}
