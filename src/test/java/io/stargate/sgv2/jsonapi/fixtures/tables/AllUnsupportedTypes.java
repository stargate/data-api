package io.stargate.sgv2.jsonapi.fixtures.tables;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;

/**
 * Table with a single primary key and all unsupported supported types NOTE: this generates a
 * table that the API does not support.
 */
public class AllUnsupportedTypes extends BaseTableFixture {

  @Override
  public TableMetadata tableMetadata(FixtureIdentifiers identifiers) {
    return builderWithTextPKAndTypes(identifiers, CqlTypesForTesting.UNSUPPORTED_FOR_INSERT)
        .build();
  }
}
