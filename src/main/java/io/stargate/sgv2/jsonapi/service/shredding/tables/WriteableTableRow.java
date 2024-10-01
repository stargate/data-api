package io.stargate.sgv2.jsonapi.service.shredding.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.*;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.Objects;

/**
 * Data that is ready to be written to a CQL table, it has been validated and we expect the insert
 * to work.
 *
 * <p>NOTE: create these using the {@link
 * io.stargate.sgv2.jsonapi.service.operation.tables.WriteableTableRowBuilder} it knows how to
 * validate and convert the data.
 *
 * <p><b>INVARIANTS:</b> For this class to be valid, the following must be true:
 *
 * <ul>
 *   <li>All the key and non primary key columns exist in the target table
 *   <li>All key columns have values.
 *   <li>All types have been converted to what he driver expects.
 * </ul>
 */
public class WriteableTableRow implements PrettyPrintable {

  private final TableSchemaObject tableSchemaObject;
  private final CqlNamedValueContainer keyColumns;
  private final CqlNamedValueContainer nonKeyColumns;
  private final CqlNamedValueContainer allColumns;

  private final RowId id;

  /**
   * Create a new instance that is valid for writing to a table.
   *
   * @param tableSchemaObject the schema of the table we are writing to, useful so we do not need to
   *     keep handing it around. Also, this class is intrinsically tied to the table schema.
   * @param keyColumns Ordered container of the {@link CqlNamedValue} that make up the primary key
   *     (partition and clustering columns), order is important for keys. Values here should not be
   *     in the nonKeyColumns.
   * @param nonKeyColumns Unordered container of the {@link CqlNamedValue} for all non primary-key
   *     columns.
   */
  public WriteableTableRow(
      TableSchemaObject tableSchemaObject,
      CqlNamedValueContainer keyColumns,
      CqlNamedValueContainer nonKeyColumns) {
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.keyColumns = Objects.requireNonNull(keyColumns, "keyColumns must not be null");
    this.nonKeyColumns = Objects.requireNonNull(nonKeyColumns, "nonKeyColumns must not be null");
    this.allColumns = new CqlNamedValueContainer();
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

  /**
   * All the columns to be inserted.
   *
   * @return an {@link CqlNamedValueContainer} that contains all the columns in the table, with the
   *     * {@link #keyColumns()} first, followed by the {@link #nonKeyColumns()}.
   */
  public CqlNamedValueContainer allColumns() {
    return allColumns;
  }

  public RowId rowId() {
    return id;
  }

  @Override
  public String toString() {
    return toString(false);
  }

  public String toString(boolean pretty) {
    return toString(new PrettyToStringBuilder(getClass(), pretty)).toString();
  }

  public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    prettyToStringBuilder
        .append("keyspace", tableSchemaObject.tableMetadata.getKeyspace())
        .append("table", tableSchemaObject.tableMetadata.getName())
        .append("keyColumns", keyColumns)
        .append("nonKeyColumns", nonKeyColumns);
    return prettyToStringBuilder;
  }

  @Override
  public PrettyToStringBuilder appendTo(PrettyToStringBuilder prettyToStringBuilder) {
    var sb = prettyToStringBuilder.beginSubBuilder(getClass());
    return toString(sb).endSubBuilder();
  }
}
