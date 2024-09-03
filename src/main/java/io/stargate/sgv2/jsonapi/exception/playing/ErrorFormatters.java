package io.stargate.sgv2.jsonapi.exception.playing;

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

public class ErrorFormatters {

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
    // TODO: this should return the API Table name for the type
    return dataType.asCql(true, true);
  }

  public static Map<String, String> errFmt(SchemaObject schemaObject) {
    return errFmt(schemaObject, null);
  }

  public static Map<String, String> errFmt(
      SchemaObject schemaObject, Consumer<Map<String, String>> consumer) {
    Map<String, String> map = new HashMap<>();
    map.put("schemaType", schemaObject.type().name());
    map.put("keyspace", schemaObject.name().keyspace());
    map.put("table", schemaObject.name().table());
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
