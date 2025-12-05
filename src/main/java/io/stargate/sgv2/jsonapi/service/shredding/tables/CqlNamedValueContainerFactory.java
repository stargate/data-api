package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.util.Strings;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import java.util.Objects;

/**
 * Builds a {@link CqlNamedValueContainer} from a {@link JsonNamedValueContainer}, using a {@link
 * JSONCodecRegistry} to create the values CQL wants and detecting any deferred values.
 */
public class CqlNamedValueContainerFactory {

  /**
   * Factory function for creating a {@link CqlNamedValue} or subtype instance.
   *
   * <p>Exists because of the {@link io.stargate.sgv2.jsonapi.service.shredding.CqlVectorNamedValue}
   */
  @FunctionalInterface
  public interface CqlNamedValueFactory {
    CqlNamedValue create(
        CqlIdentifier name,
        JSONCodecRegistry codecRegistry,
        CqlNamedValue.ErrorStrategy<? extends RequestException> errorStrategy);
  }

  private final CqlNamedValueFactory cqlNamedValueFactory;
  private final TableSchemaObject tableSchemaObject;
  private final JSONCodecRegistry codecRegistry;
  private final CqlNamedValue.ErrorStrategy<? extends RequestException> errorStrategy;

  public CqlNamedValueContainerFactory(
      TableSchemaObject tableSchemaObject,
      JSONCodecRegistry codecRegistry,
      CqlNamedValue.ErrorStrategy<? extends RequestException> errorStrategy) {
    this(CqlNamedValue::new, tableSchemaObject, codecRegistry, errorStrategy);
  }

  public CqlNamedValueContainerFactory(
      CqlNamedValueFactory cqlNamedValueFactory,
      TableSchemaObject tableSchemaObject,
      JSONCodecRegistry codecRegistry,
      CqlNamedValue.ErrorStrategy<? extends RequestException> errorStrategy) {
    this.cqlNamedValueFactory =
        Objects.requireNonNull(cqlNamedValueFactory, "cqlNamedValueFactory cannot be null");
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.codecRegistry = Objects.requireNonNull(codecRegistry, "codecRegistry cannot be null");
    this.errorStrategy = Objects.requireNonNull(errorStrategy, "errorStrategy cannot be null");
  }

  /**
   * Creates a {@link CqlNamedValueContainer} from the values in the {@link
   * JsonNamedValueContainer}.
   *
   * <p>{@link
   * io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue.ErrorStrategy#allChecks(TableSchemaObject,
   * CqlNamedValueContainer)} is called on the created container, which may throw exceptions if any
   * of the values are in an error state.
   *
   * @param source The {@link io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValue}'s to
   *     process.
   * @return The {@link CqlNamedValueContainer} with the values from the source.
   */
  public CqlNamedValueContainer create(JsonNamedValueContainer source) {

    Objects.requireNonNull(source, "source cannot be null");

    // Map everything from the JSON source into a CQL Value, we can check their state after.
    var allColumns = new CqlNamedValueContainer(source.size());
    source.forEach(
        (key, value) -> {
          var cqlIdentifier = createCqlIdentifier(key);
          var cqlNamedValue =
              cqlNamedValueFactory.create(cqlIdentifier, codecRegistry, errorStrategy);
          if (cqlNamedValue.bind(tableSchemaObject)) {
            cqlNamedValue.prepare(value);
          }
          allColumns.put(cqlNamedValue);
        });

    errorStrategy.allChecks(tableSchemaObject, allColumns);
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
