package io.stargate.sgv2.jsonapi.fixtures.tables;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;
import io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting;

/** Table with a single primary key and all supported numeric types */
public class AllNumericTypes extends BaseTableFixture {

  @Override
  public TableMetadata tableMetadata(FixtureIdentifiers identifiers) {

    return builderWithTextPKAndTypes(identifiers, CqlTypesForTesting.NUMERIC_TYPES).build();
  }
}
