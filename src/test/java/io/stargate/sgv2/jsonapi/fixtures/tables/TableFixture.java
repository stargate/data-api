package io.stargate.sgv2.jsonapi.fixtures.tables;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;

import java.util.List;

/**
 * A fixture to provide a {@link TableMetadata} for a table we want to test.
 *
 * <p>See {@link BaseTableFixture}
 */
public interface TableFixture {

  /**
   * All the table fixtures than generate table schemas we should support, unsupported ones are not
   * in this list.
   */
  List<TableFixture> SUPPORTED = List.of(
            new KeyValue(),
            new KeyValueTwoPrimaryKeys(),
            new KeyValueThreePrimaryKeys(),
            new AllNumericTypes());

  /**
   * Return the {@link TableMetadata} for the table design we want to test, using the provided
   * identifiers
   *
   * @param identifiers the identifiers to use in the table metadata, this controls how the column
   *     name are created.
   * @return the table metadata
   */
  TableMetadata tableMetadata(FixtureIdentifiers identifiers);
}
