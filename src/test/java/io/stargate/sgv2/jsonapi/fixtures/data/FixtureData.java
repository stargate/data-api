package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
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

  /**
   * Returns a Java value that we want we would have read from Jackson.
   *
   * @param type {@link DataType} to generate data for.
   * @return A Java value that we would have read from Jackson
   */
  JsonLiteral<?> fromJSON(DataType type);
}
