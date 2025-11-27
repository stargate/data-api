package io.stargate.sgv2.jsonapi.service.projection;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.*;
import java.util.function.Predicate;

/**
 * Encapsulates a map of table projection selectors for inclusion-only projections. Only tracks what
 * we want to include - exclusion logic is handled by not including items.
 */
public final class TableProjectionSelectors {
  /**
   * The map of column identifiers to their projection selectors. The presence of a selector in this
   * map means the column is included in the projection.
   */
  private final Map<CqlIdentifier, TableProjectionSelector> selectors;

  private final TableSchemaObject table;

  /**
   * Match if a column does not support reads, we can find unsupported columns from the projection.
   */
  private static final Predicate<ApiSupportDef> MATCH_READ_UNSUPPORTED =
      ApiSupportDef.Matcher.NO_MATCHES.withRead(false);

  private TableProjectionSelectors(
      Map<CqlIdentifier, TableProjectionSelector> selectors, TableSchemaObject table) {
    this.selectors = selectors;
    this.table = table;
  }

  /**
   * Create projection selectors from projection definition. Detailed logics can refer to following
   * inclusion and exclusion mode methods.
   */
  public static TableProjectionSelectors from(
      TableProjectionDefinition definition, TableSchemaObject table) {

    // check if the table columns is not readable first
    // this is to avoid trigger the UnsupportedApiDataType typeName() exception
    List<ApiColumnDef> unsupportedReadColumns =
        table.apiTableDef().allColumns().filterBySupportToList(MATCH_READ_UNSUPPORTED);
    if (!unsupportedReadColumns.isEmpty()) {
      throw ProjectionException.Code.UNSUPPORTED_COLUMN_TYPES.get(
          errVars(
              table,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(table.apiTableDef().allColumns()));
                map.put("unsupportedColumns", errFmtApiColumnDef(unsupportedReadColumns));
              }));
    }

    // create selectors based on inclusion or exclusion mode
    var selectors =
        definition.isInclusion()
            ? buildInclusionSelectors(definition, table)
            : buildExclusionSelectors(definition, table);

    return selectors;
  }

  /**
   * Build projection selectors for inclusion mode, keeping only the columns and UDT sub-fields
   * explicitly listed in the {@code definition}.
   *
   * <p>Rules: - Empty projection in inclusion mode means exclude all (returns an empty selector
   * map). - Selecting a whole UDT column (e.g. {"address": 1}) overrides any sub-field selections
   * for that column (e.g. {"address.city": 1}). - Only one-level sub-selection is supported for
   * UDTs (e.g. {"address.city": 1}). - Any unknown column or invalid sub-field path is collected
   * and results in {@link ProjectionException.Code#UNKNOWN_TABLE_COLUMNS}.
   *
   * @param definition the inclusion projection definition
   * @param table the table schema object used to validate column and UDT field names
   * @return a {@link TableProjectionSelectors} containing only explicitly included items
   * @throws ProjectionException if the definition references unknown columns or invalid UDT fields
   */
  private static TableProjectionSelectors buildInclusionSelectors(
      TableProjectionDefinition definition, TableSchemaObject table) {

    Map<CqlIdentifier, TableProjectionSelector> selectorMap = new HashMap<>();

    // include nothing, means exclude everything
    // see {@link TableProjectionDefinition#EXCLUDE_ALL_PROJECTOR}
    if (definition.getColumnNames().isEmpty()) {
      return new TableProjectionSelectors(selectorMap, table);
    }

    final ApiColumnDefContainer allColumnsInTable = table.apiTableDef().allColumns();

    // gather unknown paths for error reporting
    List<String> unknownProjectionPaths = new ArrayList<>();

    // Build selectors for explicitly included columns/fields
    for (String path : definition.getColumnNames()) {
      DocumentPath docPath = DocumentPath.from(path);
      String root = docPath.getSegment(0);
      var rootIdentifier = CqlIdentifier.fromInternal(root);
      var rootApiColumnDef = allColumnsInTable.get(rootIdentifier);

      // if root column not found, this is an invalid projection path
      if (rootApiColumnDef == null) {
        unknownProjectionPaths.add(path);
        continue;
      }

      if (docPath.getSegmentsSize() == 1) {
        // Whole column requested
        if (rootApiColumnDef.type().typeName() == ApiTypeName.UDT) {
          // this will override other sub-field selections
          // E.G. {"mainAddress": 1} overrides {"mainAddress.city": 1}
          selectorMap.put(rootIdentifier, new TableUDTProjectionSelector(rootApiColumnDef));
        } else {
          selectorMap.put(rootIdentifier, new TableProjectionSelector(rootApiColumnDef));
        }
      } else if (docPath.getSegmentsSize() > 1) {
        if (docPath.getSegmentsSize() != 2
            || rootApiColumnDef.type().typeName() != ApiTypeName.UDT) {
          // Invalid path: only UDT fields can be sub-selected, and only one level deep
          unknownProjectionPaths.add(path);
          continue;
        }
        // UDT field requested
        String subField = docPath.getSegment(1);
        ApiUdtType udtType = (ApiUdtType) rootApiColumnDef.type();
        if (!udtType
            .allFields()
            .containsKey(CqlIdentifierUtil.cqlIdentifierFromUserInput(subField))) {
          // Invalid sub-field name
          unknownProjectionPaths.add(path);
          continue;
        }
        TableUDTProjectionSelector existing =
            (TableUDTProjectionSelector) selectorMap.get(rootIdentifier);
        if (existing == null) {
          // there is no projection required for the udt column or its fields yet
          // E.G. There is no {"mainAddress.city": 1} or {"mainAddress": 1}
          TableUDTProjectionSelector selector =
              new TableUDTProjectionSelector(rootApiColumnDef, subField);
          selectorMap.put(rootIdentifier, selector);
        } else {
          // there is already a projection required for the udt column or its fields
          existing.addSubField(subField);
        }
      }
    }

    // Report unknown projection paths
    if (!unknownProjectionPaths.isEmpty()) {
      throw ProjectionException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              table,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(table.apiTableDef().allColumns()));
                map.put("unknownColumns", unknownProjectionPaths.toString());
              }));
    }

    return new TableProjectionSelectors(selectorMap, table);
  }

  /**
   * Build projection selectors for exclusion mode, starting from all table columns and removing the
   * columns and UDT sub-fields explicitly listed in the {@code definition}.
   *
   * <p>Rules: - Empty projection in exclude mode means include all (returns a selector map for
   * every column). - Excluding a whole column removes it entirely. - For UDTs, only one-level
   * sub-selection is supported (e.g. {"address.city": 0}); after exclusions, UDT selectors with no
   * remaining sub-fields are removed. - Any unknown column or invalid sub-field path is collected
   * and results in {@link ProjectionException.Code#UNKNOWN_TABLE_COLUMNS}.
   *
   * @param definition the exclusion projection definition
   * @param table the table schema object used to enumerate and validate columns and UDT fields
   * @return a {@link TableProjectionSelectors} representing all columns except the exclusions
   * @throws ProjectionException if the definition references unknown columns or invalid UDT fields
   */
  private static TableProjectionSelectors buildExclusionSelectors(
      TableProjectionDefinition definition, TableSchemaObject table) {

    // populate the selector map with all table columns/fields first
    final Map<CqlIdentifier, TableProjectionSelector> selectorMap = new HashMap<>();
    final ApiColumnDefContainer allColumnsInTable = table.apiTableDef().allColumns();
    for (Map.Entry<CqlIdentifier, ApiColumnDef> columnEntry : allColumnsInTable.entrySet()) {
      ApiColumnDef columnDef = columnEntry.getValue();
      if (columnDef.type().typeName() == ApiTypeName.UDT) {
        selectorMap.put(columnEntry.getKey(), new TableUDTProjectionSelector(columnDef));
      } else {
        selectorMap.put(columnEntry.getKey(), new TableProjectionSelector(columnDef));
      }
    }

    // exclude nothing, means include everything
    // see {@link TableProjectionDefinition#INCLUDE_ALL_PROJECTOR}
    if (definition.getColumnNames().isEmpty()) {
      return new TableProjectionSelectors(selectorMap, table);
    }

    // gather unknown paths for error reporting
    List<String> unknownProjectionPaths = new ArrayList<>();

    // remove all explicit excluded columns/fields
    for (String path : definition.getColumnNames()) {
      DocumentPath docPath = DocumentPath.from(path);
      String root = docPath.getSegment(0);
      var rootIdentifier = CqlIdentifier.fromInternal(root);
      var rootApiColumnDef = allColumnsInTable.get(rootIdentifier);

      if (rootApiColumnDef == null) {
        unknownProjectionPaths.add(path);
        continue;
      }
      if (docPath.getSegmentsSize() == 1) {
        // Whole column excluded - remove it entirely
        selectorMap.remove(rootIdentifier);
      } else if (docPath.getSegmentsSize() > 1) {
        if (docPath.getSegmentsSize() != 2
            || rootApiColumnDef.type().typeName() != ApiTypeName.UDT) {
          // Invalid path: only UDT fields can be sub-selected, and only one level deep
          unknownProjectionPaths.add(path);
          continue;
        }

        String excludedField = docPath.getSegment(1);
        TableUDTProjectionSelector udtSelector =
            (TableUDTProjectionSelector) selectorMap.get(rootIdentifier);
        udtSelector.removeSubField(excludedField);
      }
    }

    // Report unknown projection paths
    if (!unknownProjectionPaths.isEmpty()) {
      throw ProjectionException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              table,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(table.apiTableDef().allColumns()));
                map.put("unknownColumns", unknownProjectionPaths.toString());
              }));
    }

    // Clean up any UDT selectors that have no fields left after exclusions
    selectorMap
        .values()
        .removeIf(
            selector ->
                selector instanceof TableUDTProjectionSelector udtSelector
                    && udtSelector.isEmptyUdtSelector());

    return new TableProjectionSelectors(selectorMap, table);
  }

  /**
   * Compute the set of columns to include in the CQL select based on what we want to include.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Inclusion {"id": 1, "mainAddress.city": 1} → select [id, mainAddress] columns
   *   <li>Inclusion {"mainAddress": 1} → select [mainAddress] column (whole UDT)
   *   <li>Exclusion {"mainAddress.city": 0} → select [id, mainAddress, ...] (all columns; prune
   *       city later)
   *   <li>Exclusion {"id": 0, "mainAddress.city": 0} → select [mainAddress, ...] (exclude id; keep
   *       mainAddress for pruning)
   *   <li>Exclusion {"mainAddress": 0} → select [id, ...] (exclude mainAddress column entirely)
   * </ul>
   */
  public List<ColumnMetadata> toCqlColumns() {
    // Always return the root columns from our selectors
    // For inclusion mode: selectors contain only what we want
    // For exclusion mode: selectors contain what we want to keep (everything except exclusions)
    return table.tableMetadata().getColumns().values().stream()
        .filter(col -> selectors.containsKey(col.getName()))
        .toList();
  }

  /**
   * Get the selector for a specific column by its CQL identifier.
   *
   * @param columnId the CQL identifier of the column
   * @return the selector for the column, or null if no selector exists
   */
  public TableProjectionSelector getSelectorForColumn(CqlIdentifier columnId) {
    return selectors.get(columnId);
  }

  public Map<CqlIdentifier, TableProjectionSelector> getSelectors() {
    return selectors;
  }
}
