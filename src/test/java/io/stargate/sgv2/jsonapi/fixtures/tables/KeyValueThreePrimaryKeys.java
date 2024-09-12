package io.stargate.sgv2.jsonapi.fixtures.tables;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;

public class KeyValueThreePrimaryKeys extends BaseTableFixture {

  @Override
  public TableMetadata tableMetadata(FixtureIdentifiers identifiers) {
    return builder(identifiers)
        .partitionKey(identifiers.getKey(0), DataTypes.TEXT)
        .clusteringKey(identifiers.getKey(1), DataTypes.TEXT, true)
        .clusteringKey(identifiers.getKey(2), DataTypes.TEXT, false)
        .nonKeyColumn(identifiers.getColumn(3), DataTypes.TEXT)
        .build();
  }
}
