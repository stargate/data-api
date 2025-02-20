package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import java.util.List;

/**
 * Interface for a class that returns test data for a given CQL {@link DataType} used with a {@link
 * CqlFixture}.
 *
 * <p>See the {@link DefaultFixtureDataSupplier}, the idea is that we can generate test data that
 * tests min / max / missing etc and that that is used when we build out every {@link CqlFixture}
 */
public interface FixtureData {

  /** All data that should not generate an error when inserted into a table. */
  List<FixtureData> SUPPORTED =
      List.of(
          new DefaultData(),
          new MaxNumericData(),
          new MinNumericData(),
          new AllNullValues(),
          new IntegersForRealData());

  /** Data that is out of range for the type, could be under, over, or both. */
  List<FixtureData> OUT_OF_RANGE = List.of(new OverflowData());

  /** Data for types that are not supported by the API. */
  List<FixtureData> UNSUPPORTED_TYPES = List.of(new UnsupportedTypesData());

  default JsonNode fromJSON(ApiColumnDef columnDef) {
    return fromJSON(columnDef.type());
  }

  default JsonNode fromJSON(ApiDataType apiDataType) {
    return fromJSON(apiDataType.cqlType());
  }

  /**
   * Returns a JSON node we would have gotten from the document, just the value not the name
   *
   * @param type {@link DataType} to generate data for.
   * @return A JSON node, just the value not the name
   */
  JsonNode fromJSON(DataType type);
}
