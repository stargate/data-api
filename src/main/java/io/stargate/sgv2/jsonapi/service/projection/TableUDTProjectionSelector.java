package io.stargate.sgv2.jsonapi.service.projection;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Selector for projecting a UDT (user-defined type) column.
 *
 * <p>One selector is created per root column in a projection. For UDT columns, this selector tracks
 * the set of sub-fields to include or exclude, depending on the overall projection mode selected by
 * {@link TableProjectionDefinition}. When the whole UDT column is selected, this selector captures
 * all sub-fields. When individual sub-fields are selected, it captures only those sub-fields.
 *
 * <p>Examples: - Inclusion {"name": 1, "address": 1, "address.city": 1} → two selectors: one for
 * non-UDT column "name" and one UDT selector for "address". Selecting the whole UDT ("address")
 * overrides any sub-field selections such as {"address.city": 1}. - Exclusion {"address.city": 0} →
 * the UDT selector for "address" removes sub-field "city"; - Exclusion {"address.city": 0,
 * "address.country": 0}, if no sub-fields remain after exclusions, the selector is dropped.
 */
public class TableUDTProjectionSelector extends TableProjectionSelector {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * The sub-fields to include/exclude for UDT columns. If includeModeForUDTFields is true, these
   * are the fields to include. If includeModeForUDTFields is false, these are the fields to
   * exclude.
   */
  private final Set<CqlIdentifier> selectedUDTFields;

  /**
   * Create a selector for whole-UDT-column projection.
   *
   * <p>All UDT sub-fields are considered selected by default.
   *
   * @param udtColumnDef the API column definition for the UDT column
   */
  public TableUDTProjectionSelector(ApiColumnDef udtColumnDef) {
    super(udtColumnDef);
    this.selectedUDTFields = new HashSet<>();
    addAllSubFields(udtColumnDef);
  }

  /**
   * Create a selector for a UDT column when a specific sub-field is selected.
   *
   * <p>Only the provided sub-field is initially selected; additional sub-fields may be added via
   * {@link #addSubField(String)}.
   *
   * @param udtColumnDef the API column definition for the UDT column
   * @param subFieldName the initial UDT sub-field to select
   */
  public TableUDTProjectionSelector(ApiColumnDef udtColumnDef, String subFieldName) {
    super(udtColumnDef);
    this.selectedUDTFields = new HashSet<>();
    addSubField(subFieldName);
  }

  /** Select all sub-fields of the given UDT column definition. */
  private void addAllSubFields(ApiColumnDef udtColumnDef) {
    final ApiUdtType udtDataType = (ApiUdtType) udtColumnDef.type();
    for (Map.Entry<CqlIdentifier, ApiColumnDef> fieldEntry : udtDataType.allFields().entrySet()) {
      selectedUDTFields.add(fieldEntry.getKey());
    }
  }

  /** Get the selected UDT sub-fields as CQL identifiers. */
  public Set<CqlIdentifier> getSelectedUDTFields() {
    return selectedUDTFields;
  }

  /**
   * Mark a UDT sub-field as selected.
   *
   * @param subFieldName the sub-field name (as provided by user input)
   */
  public void addSubField(String subFieldName) {
    this.selectedUDTFields.add(CqlIdentifier.fromInternal(subFieldName));
  }

  /**
   * Remove a UDT sub-field from the selection (used in exclusion mode).
   *
   * @param excludedField the sub-field name to remove
   */
  public void removeSubField(String excludedField) {
    this.selectedUDTFields.remove(CqlIdentifier.fromInternal(excludedField));
  }

  /**
   * Check whether any UDT sub-fields are currently selected.
   *
   * @return {@code true} if at least one sub-field is selected; {@code false} otherwise
   */
  public boolean hasAnySubField() {
    return !selectedUDTFields.isEmpty();
  }

  /** Get the selected UDT sub-fields as internal string names. */
  public Set<String> getSubFields() {
    return selectedUDTFields.stream()
        .map(CqlIdentifier::asInternal)
        .collect(java.util.stream.Collectors.toSet());
  }

  /**
   * Returns {@code true} when this UDT selector has no selected sub-fields.
   *
   * <p>Example: UDT address (country text, city text) Projection: { "address.country": 0,
   * "address.city": 0 } // all sub-fields explicitly excluded In this case the selector is
   * considered empty and should be excluded from the projection.
   */
  public boolean isEmptyUdtSelector() {
    return selectedUDTFields.isEmpty();
  }

  /**
   * Apply this selector to the fully materialized JSON value of the UDT column.
   *
   * <p>Only the selected sub-fields are included in the returned object.
   *
   * <p>Since we are not returning sparse data, null fields are not included in the result even if
   * they are selected.
   *
   * @param fullProjectionNode the JSON node representing the full UDT value (object)
   * @return a new object node containing only the selected sub-fields
   */
  @Override
  public JsonNode projectToJsonNode(JsonNode fullProjectionNode) {
    // Inclusion mode: include only the selected fields
    ObjectNode obj = OBJECT_MAPPER.createObjectNode();
    for (String subField : getSubFields()) {
      JsonNode leaf = fullProjectionNode.get(subField);
      // Note, we are not returning sparse data
      // so null fields are not included in the result even if they are selected.
      if (leaf != null && !leaf.isNull()) {
        obj.set(subField, leaf);
      }
    }
    return obj;
  }

  /** Indicates that this selector targets a UDT column. */
  @Override
  public boolean isProjectOnUDTColumn() {
    return true;
  }
}
