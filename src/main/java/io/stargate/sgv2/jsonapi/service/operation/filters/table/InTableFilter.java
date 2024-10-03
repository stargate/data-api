package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnMetadata;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilterAnalyzedUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** API filter $in and $nin against table column. */
public class InTableFilter extends TableFilter {

  private final List<Object> arrayValue;

  private final Operator operator;

  public enum Operator {
    IN,
    // TODO NIN
    NIN
  }

  public InTableFilter(Operator operator, String path, List<Object> arrayValue) {
    super(path);
    this.arrayValue = arrayValue;
    this.operator = operator;
  }

  @Override
  public <StmtT extends OngoingWhereClause<StmtT>> StmtT apply(
      TableSchemaObject tableSchemaObject,
      StmtT ongoingWhereClause,
      List<Object> positionalValues) {
    List<Term> bindMarkers = new ArrayList<>();

    // TODO this codec part we won't do in the operation level? refer to insertOne
    //    TableInsertValuesContainer
    for (Object inValue : arrayValue) {
      try {
        var codec =
            JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
                tableSchemaObject.tableMetadata(), getPathAsCqlIdentifier(), inValue);
        positionalValues.add(codec.toCQL(inValue));
        bindMarkers.add(bindMarker());
      } catch (UnknownColumnException e) {
        throw FilterException.Code.UNKNOWN_TABLE_COLUMNS.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtColumnMetadata(
                          tableSchemaObject.tableMetadata().getColumns().values()));
                  map.put("unknownColumns", path);
                }));
      } catch (MissingJSONCodecException e) {
        throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtColumnMetadata(
                          tableSchemaObject.tableMetadata().getColumns().values()));
                  map.put("unsupportedColumns", path);
                }));
      } catch (ToCQLCodecException e) {
        throw new RuntimeException(e);
      }
    }

    return ongoingWhereClause.where(Relation.column(getPathAsCqlIdentifier()).in(bindMarkers));
  }

  /**
   * This is an override method from DBFilterBase interface. When we migrate collection filter path
   * into the new design, this method may need to be implemented.
   *
   * @return BuiltCondition
   */
  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException(
        "Not supported - will be modified when we migrate collections filters java driver");
  }

  /**
   * Analyze the $in API table filter and get corresponding usage info.
   *
   * <p>[$in against scalar columns] Without SAI index, following 14 column types
   * text/int/timestamp/ascii/date/time/timestamp/boolean/varint/tinyint/decimal/smallint/double/bigint/float
   * need ALLOW FILTERING. <br>
   * We can NOT build SAI index on duration column type, so ALLOW FILTERING is also needed. TODO,
   * blob column
   *
   * <p>[$in against collection columns] TODO, collection columns
   *
   * @param tableSchemaObject tableSchemaObject
   * @return TableFilterAnalyzedUsage
   */
  @Override
  public TableFilterAnalyzedUsage analyze(TableSchemaObject tableSchemaObject) {
    // check if filter is against an existing column
    final ColumnMetadata column = getColumn(tableSchemaObject);

    // Against a scalar column.
    // For scalar column, 15 types(no blob yet), if no SAI index on the column, need ALLOW FILTERING
    if (!hasSaiIndexOnColumn(tableSchemaObject)) {
      return new TableFilterAnalyzedUsage(path, true, Optional.of("ALLOW FILTERING turned on"));
      // TODO, blob
    }

    return new TableFilterAnalyzedUsage(path, false, Optional.empty());

    // TODO, Against a collection column.
  }
}
