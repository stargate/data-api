package io.stargate.sgv2.jsonapi.fixtures.tables;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;

public class KeyValue extends BaseTableFixture {

  @Override
  public TableMetadata tableMetadata(FixtureIdentifiers identifiers) {
    return builder(identifiers)
        .partitionKey(identifiers.getKey(0), DataTypes.TEXT)
        .nonKeyColumn(identifiers.getColumn(1), DataTypes.TEXT)
        .build();
  }
}
