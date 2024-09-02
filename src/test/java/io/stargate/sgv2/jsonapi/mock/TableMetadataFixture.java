package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * A fixture to provide a {@link TableMetadata} for a table we want to test.
 *
 * <p>See {@link TableMetadataFixtureSource}
 */
public interface TableMetadataFixture {

  /**
   * Return the {@link TableMetadata} for the table design we want to test, using the provided
   * identifiers
   *
   * @param identifiers the identifiers to use in the table metadata, this controls how the column
   *     name are created.
   * @return the table metadata
   */
  TableMetadata tableMetadata(CqlIdentifiers identifiers);
}
