package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Interface for a class that returns test data for a given CQL {@link DataType} used with a {@link
 * CqlFixture}.
 *
 * <p>See the {@link CqlDataSource}, the idea is that we can generate test data that tests min / max
 * / missing etc and that that is used when we build out every {@link CqlFixture}
 */
public interface CqlData {

  /**
   * Returns a Java value that we want we would have ready from Jackson.
   *
   * @param type
   * @return
   */
  Object fromJSON(DataType type);
}
