package io.stargate.sgv2.jsonapi.fixtures.tables;

import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.join;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Builder for creating a {@link TableMetadata} from the C* java driver, creates an instance of the
 * {@link DefaultTableMetadata} the driver uses.
 *
 * <p>Used by the {@link BaseTableFixture} to create tables with specific designs, e.g. <code>
 *   return new TableMetadataBuilder<>()
 *         .keyspace(identifiers.randomKeyspace())
 *         .table(identifiers.randomTable());
 *         .partitionKey(identifiers.getKey(0), DataTypes.TEXT)
 *         .clusteringKey(identifiers.getColumn(1), DataTypes.TEXT, true)
 *         .nonKeyColumn(identifiers.getColumn(2), DataTypes.TEXT)
 *         .build();
 * </code>
 *
 * <p>
 *
 * @param <T> used for Fluent API,just use the <code>?</code> if you need to define a type
 */
public class TableMetadataBuilder<T extends TableMetadataBuilder> {

  private CqlIdentifier keyspace;
  private CqlIdentifier table;
  private final List<ColumnDef> partitionKeys = new ArrayList<>();
  private final List<ClusteringDef> clusteringKeys = new ArrayList<>();
  private final List<ColumnDef> nonKeyColumns = new ArrayList<>();

  /**
   * Helper for renaming a column in a {@link ColumnMetadata} object, here just to keep the work
   * with driver metadata in this class.
   */
  public static ColumnMetadata renameColumn(ColumnMetadata columnMetadata, CqlIdentifier newName) {
    return new DefaultColumnMetadata(
        columnMetadata.getKeyspace(),
        columnMetadata.getParent(),
        newName,
        columnMetadata.getType(),
        columnMetadata.isStatic());
  }

  @SuppressWarnings("unchecked")
  protected T _this() {
    return (T) this;
  }

  public T keyspace(CqlIdentifier keyspace) {
    this.keyspace = keyspace;
    return _this();
  }

  public T table(CqlIdentifier table) {
    this.table = table;
    return _this();
  }

  public T partitionKey(CqlIdentifier name, DataType type) {
    return partitionKeys(new ColumnDef(name, type));
  }

  public T partitionKeys(ColumnDef... partitionKeys) {
    this.partitionKeys.addAll(Arrays.asList(partitionKeys));
    return _this();
  }

  public T clusteringKey(CqlIdentifier name, DataType type, boolean ascending) {
    return clusteringKeys(new ClusteringDef(new ColumnDef(name, type), ascending));
  }

  public T clusteringKeys(ClusteringDef... clusteringKeys) {
    this.clusteringKeys.addAll(Arrays.asList(clusteringKeys));
    return _this();
  }

  public T nonKeyColumn(CqlIdentifier name, DataType type) {
    return nonKeyColumns(new ColumnDef(name, type));
  }

  public T nonKeyColumns(ColumnDef... otherColumns) {
    this.nonKeyColumns.addAll(Arrays.asList(otherColumns));
    return _this();
  }

  public TableMetadata build() {

    // check there are no dups columns names, easy mistake to mack
    var allColumnsWithDup = new ArrayList<ColumnDef>();
    allColumnsWithDup.addAll(partitionKeys);
    allColumnsWithDup.addAll(clusteringKeys.stream().map(ClusteringDef::columnDef).toList());
    allColumnsWithDup.addAll(nonKeyColumns);
    var allColumnsNoDup = new HashSet<CqlIdentifier>();
    allColumnsWithDup.forEach(
        columnDef -> {
          if (!allColumnsNoDup.add(columnDef.name())) {
            throw new IllegalArgumentException("Duplicate column name: " + columnDef.name());
          }
        });

    var partitionKeyMetadata = ColumnDef.toList(partitionKeys, keyspace, table);
    var clusteringKeyMetadata = ClusteringDef.toMap(clusteringKeys, keyspace, table);
    var nonKeyMetadata = ColumnDef.toList(this.nonKeyColumns, keyspace, table);
    var allColumnsMetadata =
        join(partitionKeyMetadata, clusteringKeyMetadata.keySet(), nonKeyMetadata).stream()
            .collect(Collectors.toMap(ColumnMetadata::getName, Function.identity()));

    return new DefaultTableMetadata(
        keyspace,
        table,
        UUID.randomUUID(),
        false, // not compact storage, that is old
        false, // not virtual
        partitionKeyMetadata,
        clusteringKeyMetadata,
        allColumnsMetadata,
        Collections.emptyMap(), // Options
        Collections.emptyMap() // indexes
        );
  }

  public record ColumnDef(CqlIdentifier name, DataType type) {

    public ColumnMetadata columnMetadata(CqlIdentifier keyspace, CqlIdentifier table) {
      return new DefaultColumnMetadata(keyspace, table, name, type, false);
    }

    public static List<ColumnMetadata> toList(
        List<ColumnDef> columnDefs, CqlIdentifier keyspace, CqlIdentifier table) {
      return columnDefs.stream().map(def -> def.columnMetadata(keyspace, table)).toList();
    }
  }

  public record ClusteringDef(ColumnDef columnDef, boolean ascending) {

    public ClusteringOrder clusteringOrder() {
      return ascending ? ClusteringOrder.ASC : ClusteringOrder.DESC;
    }

    public static Map<ColumnMetadata, ClusteringOrder> toMap(
        List<ClusteringDef> clusteringDefs, CqlIdentifier keyspace, CqlIdentifier table) {
      return clusteringDefs.stream()
          .collect(
              Collectors.toMap(
                  clusteringDef -> clusteringDef.columnDef.columnMetadata(keyspace, table),
                  ClusteringDef::clusteringOrder));
    }
  }
}
