package io.stargate.sgv2.jsonapi.service.shredding.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.*;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;

import java.util.Objects;

/**
 * The data extracted from a JSON document that can be written to a table row.
 *
 * <p>Created by the {@link RowShredder}
 *
 * @param id
 * @param allColumnValues
 */
public class WriteableTableRow implements WritableDocRow {

  private final TableSchemaObject tableSchemaObject;
  private final OrderedCqlNamedValueContainer keyColumns;
  private final UnorderedCqlNamedValueContainer nonKeyColumns;
  private final OrderedCqlNamedValueContainer allColumns;

  private final RowId id;

  public WriteableTableRow(
      TableSchemaObject tableSchemaObject,
      OrderedCqlNamedValueContainer keyColumns,
      UnorderedCqlNamedValueContainer nonKeyColumns) {
    this.tableSchemaObject = Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.keyColumns = Objects.requireNonNull(keyColumns, "keyColumns must not be null");
    this.nonKeyColumns = Objects.requireNonNull(nonKeyColumns, "nonKeyColumns must not be null");
    this.allColumns = new OrderedCqlNamedValueContainer();
    this.allColumns.putAll(keyColumns);
    this.allColumns.putAll(nonKeyColumns);

    // HACK TODO Aaron - for now the primary key is an array , this may change
    this.id = new RowId(keyColumns.valuesValue().toArray());
  }

  public CqlNamedValueContainer keyColumns() {
    return keyColumns;
  }

  public CqlNamedValueContainer nonKeyColumns() {
    return nonKeyColumns;
  }

  public CqlNamedValueContainer allColumns() {
    return nonKeyColumns;
  }


  @Override
  public DocRowIdentifer docRowID() {
    return id;
  }

  public RowId rowId() {
    return id;
  }

  @Override
  public String toString() {
    return toString(false);
  }

  public String toString(boolean pretty) {
    var sb = new PrettyToStringBuilder(getClass(), pretty);
    sb.append("keyspace", tableSchemaObject.tableMetadata.getKeyspace())
        .append("table", tableSchemaObject.tableMetadata.getName())
        .append("keyColumns", keyColumns)
        .append("nonKeyColumns", nonKeyColumns);
    return sb.toString();
  }
}