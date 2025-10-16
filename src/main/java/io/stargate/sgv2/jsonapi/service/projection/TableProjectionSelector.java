package io.stargate.sgv2.jsonapi.service.projection;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;

/**
 * Selector for a root table column projection.
 *
 * <p>This base selector models projection for non-UDT (non-user-defined type) columns where the
 * whole column is either included or excluded at the root level. For UDT columns that support
 * per-field sub-selection, see {@link TableUDTProjectionSelector}.
 *
 * <p>Responsibilities: - Identify the target column via its {@link CqlIdentifier} and {@link
 * ApiColumnDef}. - For non-UDT columns, project the full value (no sub-field pruning). - Act as a
 * common type used by {@link TableProjectionSelectors} during inclusion/exclusion resolution.
 */
public class TableProjectionSelector {

  private final CqlIdentifier columnIdentifier;

  private final ApiColumnDef rootColumnDef;

  /**
   * Create a selector for whole-column projection.
   *
   * @param columnDef the API column definition; must represent a non-UDT column for this base
   *     selector. UDT columns should use {@link TableUDTProjectionSelector} instead
   */
  public TableProjectionSelector(ApiColumnDef columnDef) {
    this.columnIdentifier = columnDef.name();
    this.rootColumnDef = columnDef;
  }

  /**
   * Whether this selector targets a UDT column.
   *
   * <p>Base implementation returns {@code false}. {@link TableUDTProjectionSelector} overrides and
   * returns {@code true} to indicate UDT-specific handling is required.
   */
  public boolean isProjectOnUDTColumn() {
    return false;
  }

  /**
   * Apply this selector to the fully-materialized JSON value for the column.
   *
   * <p>For non-UDT columns, there is no sub-field pruning; the value is returned unchanged to
   * represent whole-column projection. UDT-specific pruning logic is implemented in {@link
   * TableUDTProjectionSelector}.
   *
   * @param fullProjectionNode the JSON node representing the full value of the column
   * @return the projected JSON node (unchanged for non-UDT columns)
   */
  public JsonNode projectToJsonNode(JsonNode fullProjectionNode) {
    return fullProjectionNode;
  }

  /** Get the API column definition for the root column targeted by this selector. */
  public ApiColumnDef getColumnDef() {
    return rootColumnDef;
  }

  /** Get the CQL identifier of the root column targeted by this selector. */
  public CqlIdentifier getColumnIdentifier() {
    return columnIdentifier;
  }
}
