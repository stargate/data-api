package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtCqlIdentifier;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.TableDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKeyDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TableDefinitionDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.*;

/**
 * The APi model for a table in the database.
 *
 * <p>Created by the factories {@link #FROM_TABLE_DESC_FACTORY} and {@link #FROM_CQL_FACTORY}.
 */
public class ApiTableDef {

  public static final FromTableDescFactory FROM_TABLE_DESC_FACTORY = new FromTableDescFactory();
  public static final FromCqlFactory FROM_CQL_FACTORY = new FromCqlFactory();

  private final CqlIdentifier name;
  private final ApiColumnDefContainer primaryKeys;
  private final ApiColumnDefContainer partitionkeys;
  private final List<ApiClusteringDef> clusteringDefs;
  private final ApiColumnDefContainer clusteringKeys;
  private final ApiColumnDefContainer allColumns;
  private final ApiColumnDefContainer nonPKColumns;
  private final ApiColumnDefContainer unsupportedColumns;

  // split into two so we do not accidentally reference an index we cannot use.
  private final ApiIndexDefContainer supportedIndexes;
  private final ApiIndexDefContainer indexesIncludingUnsupported;

  private ApiTableDef(
      CqlIdentifier name,
      ApiColumnDefContainer primaryKeys,
      ApiColumnDefContainer partitionkeys,
      List<ApiClusteringDef> clusteringDefs,
      ApiColumnDefContainer allColumns,
      ApiIndexDefContainer allIndexes) {

    this.name = name;
    this.primaryKeys = primaryKeys.toUnmodifiable();
    this.partitionkeys = partitionkeys.toUnmodifiable();
    this.clusteringDefs = Collections.unmodifiableList(clusteringDefs);
    this.clusteringKeys =
        ApiColumnDefContainer.of(clusteringDefs.stream().map(ApiClusteringDef::columnDef).toList());
    this.allColumns = allColumns.toUnmodifiable();
    this.supportedIndexes = allIndexes.filterBySupported();
    this.indexesIncludingUnsupported = allIndexes;

    var workingNonPKColumns = new ApiColumnDefContainer(allColumns().size() - primaryKeys.size());
    allColumns.values().stream()
        .filter(columnDef -> !primaryKeys.contains(columnDef))
        .forEach(workingNonPKColumns::put);
    this.nonPKColumns = workingNonPKColumns.toUnmodifiable();

    this.unsupportedColumns =
        allColumns.values().stream()
            .filter(columnDef -> columnDef.type().isUnsupported())
            .collect(
                ApiColumnDefContainer::new,
                ApiColumnDefContainer::put,
                ApiColumnDefContainer::putAll);
  }

  /**
   * Converts the table definition to a table description for the user.
   *
   * <p>This is used to send the table definition to the user.
   *
   * @return the {@link TableDesc} for the table.
   */
  public TableDesc toTableDesc() {

    var partitionKeys = partitionkeys.values().stream().map(ApiColumnDef::name).toList();
    var orderingKeys =
        clusteringDefs.stream()
            .map(
                clusteringDef ->
                    PrimaryKeyDesc.OrderingKeyDesc.from(
                        clusteringDef.columnDef().name(), clusteringDef.order()))
            .toList();
    var primaryKey = PrimaryKeyDesc.from(partitionKeys, orderingKeys);

    var columnsDesc = new ColumnsDescContainer();
    allColumns
        .values()
        .forEach(columnDef -> columnsDesc.put(columnDef.name(), columnDef.type().columnDesc()));

    return io.stargate.sgv2.jsonapi.api.model.command.table.TableDesc.from(
        name, new TableDefinitionDesc(columnsDesc, primaryKey));
  }

  /** Get the name for this table. */
  public CqlIdentifier name() {
    return name;
  }

  /**
   * Get the primary keys for this table, these are the partition keys and the clustering keys in
   * order.
   */
  public ApiColumnDefContainer primaryKeys() {
    return primaryKeys;
  }

  /** Get the partition keys for this table, in order. */
  public ApiColumnDefContainer partitionKeys() {
    return partitionkeys;
  }

  /**
   * Get the clustering key definitions that includes the {@link ApiClusteringOrder} for this table,
   * in order.
   */
  public List<ApiClusteringDef> clusteringDefs() {
    return clusteringDefs;
  }

  /**
   * Get just the columns in the clustering keys for this table, in order but excluding the {@link
   * ApiClusteringOrder} .
   */
  public ApiColumnDefContainer clusteringKeys() {
    return clusteringKeys;
  }

  /** Get all columns in the table, including the columns in the primary key. */
  public ApiColumnDefContainer allColumns() {
    return allColumns;
  }

  /** Get all the columns in the table that are not part of the primary key. */
  public ApiColumnDefContainer nonPKColumns() {
    return nonPKColumns;
  }

  /**
   * Get all the columns in the table that are in some way not supported by the API, such as being a
   * UDT etc. These are tracked for tables that were created outside of the API.
   */
  public ApiColumnDefContainer unsupportedColumns() {
    return unsupportedColumns;
  }

  /** Get all the indexes on this table that are supported by the API */
  public ApiIndexDefContainer indexes() {
    return supportedIndexes;
  }

  /** Gets all the indexes on the table that are supported and unsupported by the API. */
  public ApiIndexDefContainer indexesIncludingUnsupported() {
    return indexesIncludingUnsupported;
  }

  /**
   * Factory for creating a {@link ApiTableDef} from a users decription sent in a command {@link
   * TableDefinitionDesc}.
   *
   * <p>Use the singleton {@link #FROM_TABLE_DESC_FACTORY} to create an instance.
   */
  public static final class FromTableDescFactory extends FactoryFromDesc {

    FromTableDescFactory() {}

    public ApiTableDef create(
        String name, TableDefinitionDesc tableDesc, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(tableDesc, "tableDesc must not be null");

      var tableIdentifier = userNameToIdentifier(name, "tableName");

      // Parsing the user description of what they want

      var allColumnDefs =
          ApiColumnDefContainer.FROM_COLUMN_DESC_FACTORY.create(
              tableDesc.columns(), validateVectorize);
      var partitionIdentifiers =
          Arrays.stream(tableDesc.primaryKey().keys())
              .map(CqlIdentifierUtil::cqlIdentifierFromUserInput)
              .toList();

      // just removing the nullable
      List<PrimaryKeyDesc.OrderingKeyDesc> clusteringKeysDec =
          tableDesc.primaryKey().orderingKeys() == null
              ? List.of()
              : Arrays.stream(tableDesc.primaryKey().orderingKeys()).toList();
      List<PrimaryKeyDesc.OrderingKeyDesc> missingPartitionSort = new ArrayList<>();
      List<ApiClusteringDef> clusteringDefs =
          clusteringKeysDec.stream()
              .map(
                  clusteringKeyDec -> {
                    var maybeDef =
                        ApiClusteringDef.FROM_USER_DESC_FACTORY.create(
                            allColumnDefs, clusteringKeyDec);
                    if (maybeDef.isEmpty()) {
                      missingPartitionSort.add(clusteringKeyDec);
                    }
                    return maybeDef;
                  })
              .filter(Optional::isPresent)
              .map(Optional::get)
              .toList();

      // Validation of the user description

      // Have to this here, or we would need a type of ApiClusteringDef that was invalid
      if (!missingPartitionSort.isEmpty()) {
        throw SchemaException.Code.UNKNOWN_PARTITION_SORT_COLUMNS.get(
            Map.of(
                "tableColumns",
                errFmtCqlIdentifier(allColumnDefs.keySet()),
                "unknownColumns",
                errFmtJoin(
                    missingPartitionSort.stream()
                        .map(PrimaryKeyDesc.OrderingKeyDesc::column)
                        .toList())));
      }

      // TODO: not sure about the validation happening here, was part of the code I moved
      // should be in the builder for the attempt
      if (partitionIdentifiers.isEmpty()) {
        throw SchemaException.Code.ZERO_PARTITION_COLUMNS.get(
            Map.of("tableColumns", errFmtCqlIdentifier(allColumnDefs.keySet())));
      }

      var unknownPartitionColumns =
          partitionIdentifiers.stream().filter(key -> !allColumnDefs.containsKey(key)).toList();
      if (!unknownPartitionColumns.isEmpty()) {
        throw SchemaException.Code.UNKNOWN_PARTITION_COLUMNS.get(
            Map.of(
                "tableColumns",
                errFmtCqlIdentifier(allColumnDefs.keySet()),
                "unknownColumns",
                errFmtCqlIdentifier(unknownPartitionColumns)));
      }

      // NOTE: no check on duplicate use of columns in the partition key or in the clustering key
      // There could be valid reasons for wanting to do this.

      // Building the table definition

      // checked above that all the partition keys are in the columns
      var partitionKeys =
          partitionIdentifiers.stream()
              .map(allColumnDefs::get)
              .collect(
                  ApiColumnDefContainer::new,
                  ApiColumnDefContainer::put,
                  ApiColumnDefContainer::putAll);

      var primaryKeys = new ApiColumnDefContainer(partitionKeys.size() + clusteringDefs.size());
      partitionKeys.values().forEach(primaryKeys::put);
      clusteringDefs.forEach(clusteringDefn -> primaryKeys.put(clusteringDefn.columnDef()));

      // when creating the table from the User Desc it does not include any indexes
      return new ApiTableDef(
          tableIdentifier,
          primaryKeys,
          partitionKeys,
          clusteringDefs,
          allColumnDefs,
          ApiIndexDefContainer.of());
    }
  }

  /**
   * Factory for creating a {@link ApiTableDef} from a {@link TableMetadata} object.
   *
   * <p>Use the singleton {@link #FROM_CQL_FACTORY} to create an instance.
   */
  public static class FromCqlFactory {

    FromCqlFactory() {}

    public ApiTableDef create(TableMetadata tableMetadata, VectorConfig vectorConfig) {

      Objects.requireNonNull(tableMetadata, "tableMetadata cannot be null");
      Objects.requireNonNull(vectorConfig, "vectorConfig cannot be null");

      var allColumns =
          ApiColumnDefContainer.FROM_CQL_FACTORY.create(
              tableMetadata.getColumns().values(), vectorConfig);

      // TODO: add validation that the columns are found in all columns
      var primaryKeys = new ApiColumnDefContainer(tableMetadata.getPrimaryKey().size());
      tableMetadata.getPrimaryKey().stream()
          .map(metadata -> allColumns.get(metadata.getName()))
          .forEach(primaryKeys::put);

      var partitionKeys = new ApiColumnDefContainer(tableMetadata.getPartitionKey().size());
      tableMetadata.getPartitionKey().stream()
          .map(metadata -> allColumns.get(metadata.getName()))
          .forEach(partitionKeys::put);

      var clusteringKeys =
          new ArrayList<ApiClusteringDef>(tableMetadata.getClusteringColumns().size());
      tableMetadata.getClusteringColumns().entrySet().stream()
          .map(
              entry ->
                  ApiClusteringDef.FROM_CQL_FACTORY.create(
                      allColumns.get(entry.getKey().getName()), entry.getValue()))
          .forEach(clusteringKeys::add);

      var apiIndexes =
          ApiIndexDefContainer.FROM_CQL_FACTORY.create(
              allColumns, tableMetadata.getIndexes().values());

      return new ApiTableDef(
          tableMetadata.getName(),
          primaryKeys,
          partitionKeys,
          clusteringKeys,
          allColumns,
          apiIndexes);
    }
  }
}
