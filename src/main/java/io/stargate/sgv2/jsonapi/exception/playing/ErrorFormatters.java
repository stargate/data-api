package io.stargate.sgv2.jsonapi.exception.playing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants.TemplateVars;
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
 * <p>Probably want to use the {@link #errVars(SchemaObject, Consumer)} normally, and then call the
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

  public static Map<String, String> errVars(SchemaObject schemaObject) {
    return errVars(schemaObject, null);
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
  public static Map<String, String> errVars(
      SchemaObject schemaObject, Consumer<Map<String, String>> consumer) {
    Map<String, String> map = new HashMap<>();
    map.put(TemplateVars.SCHEMA_TYPE, schemaObject.type.name());
    map.put(TemplateVars.KEYSPACE, schemaObject.name.keyspace());
    map.put(TemplateVars.TABLE, schemaObject.name.table());
    if (consumer != null) {
      consumer.accept(map);
    }
    return map;
  }

  public static Map<String, String> errVars(Throwable runtimeException) {
    return errVars(runtimeException, null);
  }

  public static Map<String, String> errVars(
      Throwable runtimeException, Consumer<Map<String, String>> consumer) {
    Map<String, String> map = new HashMap<>();
    map.put(TemplateVars.ERROR_CLASS, runtimeException.getClass().getSimpleName());
    map.put(TemplateVars.ERROR_MESSAGE, runtimeException.getMessage());
    if (consumer != null) {
      consumer.accept(map);
    }
    return map;
  }
}
