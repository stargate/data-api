package io.stargate.sgv2.jsonapi.fixtures.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;

import java.util.List;

/**
 * Provides implementations of the {@link TableFixture} that generate tables of specified
 * designs.
 *
 * <p>Create inner subclasses that extend this class and add to the {@link #SUPPORTED}
 * list to have them included in the test runs by the {@link CqlFixture}. If you create tables that
 * are supported do not add them to the list.
 *
 * <p>The name of the subclass is used in the test description.
 */
abstract class BaseTableFixture implements TableFixture {


  /**
   * Override so in test output it has the fixture name, not the full class name
   *
   * @return
   */
  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  /**
   * helper for sub-classes to return a builder with the keyspace and table set, and any other basic
   * setup.
   */
  protected TableMetadataBuilder<?> builder(FixtureIdentifiers identifiers) {
    return new TableMetadataBuilder<>()
        .keyspace(identifiers.randomKeyspace())
        .table(identifiers.randomTable());
  }

  protected TableMetadataBuilder<?> builderWithTextPKAndTypes(FixtureIdentifiers identifiers, List<DataType> types) {
    var builder = builder(identifiers)
        .partitionKey(identifiers.getKey(0), DataTypes.TEXT);

    int i = 1;
    for (var type : types) {
      builder.nonKeyColumn(identifiers.getColumn(i++), type);
    }
    return builder;
  }
}
