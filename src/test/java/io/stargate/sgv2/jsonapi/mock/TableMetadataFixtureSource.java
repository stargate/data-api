package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.List;

/**
 * Provides implementations of the {@link TableMetadataFixture} that generate tables of specified
 * designs.
 *
 * <p>Create inner subclasses that extend this class and add to the {@link #ALL_SUPPORTED_SOURCES}
 * list to have them included in the test runs by the {@link CqlFixture}. If you create tables that
 * are supported do not add them to the list.
 *
 * <p>The name of the subclass is used in the test description.
 */
public abstract class TableMetadataFixtureSource implements TableMetadataFixture {

  /**
   * All the table fixtures than generate table schemas we should support, unsupported ones are not
   * in this list.
   */
  public static final List<TableMetadataFixture> ALL_SUPPORTED_SOURCES;

  static {
    ALL_SUPPORTED_SOURCES =
        List.of(
            new KeyValue(),
            new KeyValueTwoPrimaryKeys(),
            new KeyValueThreePrimaryKeys(),
            new AllNumericTypes());
  }

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
  protected TableMetadataBuilder<?> builder(CqlIdentifiers identifiers) {
    return new TableMetadataBuilder<>()
        .keyspace(identifiers.randomKeyspace())
        .table(identifiers.randomTable());
  }

  public static class KeyValue extends TableMetadataFixtureSource {

    @Override
    public TableMetadata tableMetadata(CqlIdentifiers identifiers) {
      return builder(identifiers)
          .partitionKey(identifiers.getKey(0), DataTypes.TEXT)
          .nonKeyColumn(identifiers.getColumn(1), DataTypes.TEXT)
          .build();
    }
  }

  public static class KeyValueTwoPrimaryKeys extends TableMetadataFixtureSource {

    @Override
    public TableMetadata tableMetadata(CqlIdentifiers identifiers) {
      return builder(identifiers)
          .partitionKey(identifiers.getKey(0), DataTypes.TEXT)
          .clusteringKey(identifiers.getColumn(1), DataTypes.TEXT, true)
          .nonKeyColumn(identifiers.getColumn(2), DataTypes.TEXT)
          .build();
    }
  }

  public static class KeyValueThreePrimaryKeys extends TableMetadataFixtureSource {

    @Override
    public TableMetadata tableMetadata(CqlIdentifiers identifiers) {
      return builder(identifiers)
          .partitionKey(identifiers.getKey(0), DataTypes.TEXT)
          .clusteringKey(identifiers.getKey(1), DataTypes.TEXT, true)
          .clusteringKey(identifiers.getKey(2), DataTypes.TEXT, false)
          .nonKeyColumn(identifiers.getColumn(3), DataTypes.TEXT)
          .build();
    }
  }

  /** Table with a single primary key and all supported numeric types */
  public static class AllNumericTypes extends TableMetadataFixtureSource {

    @Override
    public TableMetadata tableMetadata(CqlIdentifiers identifiers) {
      var builder = builder(identifiers).partitionKey(identifiers.getKey(0), DataTypes.TEXT);

      int i = 1;
      for (var type : CqlTypesForTesting.NUMERIC_TYPES) {
        builder.nonKeyColumn(identifiers.getColumn(i++), type);
      }
      return builder.build();
    }
  }

  /**
   * Table with a single primary key and all unsupported supported types NOTE: this generates a
   * table that the API does not support.
   */
  public static class AllUnsupportedTypes extends TableMetadataFixtureSource {

    @Override
    public TableMetadata tableMetadata(CqlIdentifiers identifiers) {
      var builder = builder(identifiers).partitionKey(identifiers.getKey(0), DataTypes.TEXT);

      int i = 1;
      for (var type : CqlTypesForTesting.UNSUPPORTED_FOR_INSERT) {
        builder.nonKeyColumn(identifiers.getColumn(i++), type);
      }
      return builder.build();
    }
  }
}
