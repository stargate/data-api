package io.stargate.sgv2.jsonapi.fixtures.tables;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;
import io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting;

public class AllUnderflowTypes extends BaseTableFixture {

  @Override
  public TableMetadata tableMetadata(FixtureIdentifiers identifiers) {

    return builderWithTextPKAndTypes(identifiers, CqlTypesForTesting.UNDERFLOW_TYPES).build();
  }
}
