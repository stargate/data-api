package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.util.Strings;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import java.util.Objects;

/**
 * Builds a {@link CqlNamedValueContainer} from a {@link JsonNamedValueContainer}.
 *
 * <p>The caller is responsible for checking the state of the values in the returned {@link
 * CqlNamedValueContainer}.
 */
public class CqlNamedValueFactory {

  private final TableSchemaObject tableSchemaObject;
  private final JSONCodecRegistry codecRegistry;
  private final CqlNamedValue.ErrorStrategy<? extends RequestException> errorStrategy;

  public CqlNamedValueFactory(
      TableSchemaObject tableSchemaObject,
      JSONCodecRegistry codecRegistry,
      CqlNamedValue.ErrorStrategy<? extends RequestException> errorStrategy) {

    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.codecRegistry = Objects.requireNonNull(codecRegistry, "codecRegistry cannot be null");
    this.errorStrategy = Objects.requireNonNull(errorStrategy, "errorStrategy cannot be null");
  }

  public CqlNamedValueContainer create(JsonNamedValueContainer source) {

    Objects.requireNonNull(source, "source cannot be null");

    Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    Objects.requireNonNull(codecRegistry, "codecRegistry cannot be null");
    Objects.requireNonNull(errorStrategy, "errorStrategy cannot be null");

    // Map everything from the JSON source into a CQL Value, we can check their state after.
    var allColumns = new CqlNamedValueContainer(source.size());
    source.forEach(
        (key, value) -> {
          var cqlIdentifier = createCqlIdentifier(key);
          var cqlNamedValue = new CqlNamedValue(cqlIdentifier, codecRegistry, errorStrategy);
          if (cqlNamedValue.bind(tableSchemaObject)) {
            cqlNamedValue.prepare(value);
          }
          allColumns.put(cqlNamedValue);
        });

    return allColumns;
  }

  /**
   * Uses similar logic to the {@link CqlIdentifier#fromCql(String)} and double quotes the string if
   * it is not already quoted.
   *
   * <p>aaron - Feb 19th 2025 - this was oroginally in the WriteableTableRowBuilder, kept as is
   * until we know if we need to change it.
   */
  private static CqlIdentifier createCqlIdentifier(JsonPath name) {
    if (Strings.isDoubleQuoted(name.toString())) {
      return CqlIdentifier.fromCql(name.toString());
    }
    return CqlIdentifier.fromCql(Strings.doubleQuote(name.toString()));
  }
}
