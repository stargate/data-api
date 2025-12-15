package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnMetadata;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.ArrayList;
import java.util.List;

/** API filter $in and $nin against table column. */
public class InTableFilter extends TableFilter {

  // TODO: this is prob sub-class NativeTypeTableFilter because it is not against a collection
  private final List<Object> arrayValue;

  public final Operator operator;

  public enum Operator {
    IN,
    NIN;

    Operator() {}

    public static InTableFilter.Operator from(ValueComparisonOperator operator) {
      return switch (operator) {
        case IN -> IN;
        case NIN -> NIN;
        default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
      };
    }
  }

  public InTableFilter(Operator operator, String path, List<Object> arrayValue) {
    super(path, null);
    this.arrayValue = arrayValue;
    this.operator = operator;
  }

  @Override
  public Relation apply(TableSchemaObject tableSchemaObject, List<Object> positionalValues) {

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
        throw FilterException.Code.INVALID_FILTER_COLUMN_VALUES.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtColumnMetadata(
                          tableSchemaObject.tableMetadata().getColumns().values()));
                  map.put("invalidColumn", path);
                  map.put(
                      "columnType",
                      tableSchemaObject
                          .tableMetadata()
                          .getColumn(CqlIdentifier.fromInternal(path))
                          .get()
                          .getType()
                          .toString());
                }));
      }
    }

    return applyInOperator(Relation.column(getPathAsCqlIdentifier()), bindMarkers);
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

  private Relation applyInOperator(
      ColumnRelationBuilder<Relation> columnRelationBuilder, List<Term> bindMarkers) {
    return switch (operator) {
      case IN -> columnRelationBuilder.in(bindMarkers);
      case NIN -> columnRelationBuilder.notIn(bindMarkers);
    };
  }

  @Override
  public boolean filterIsExactMatch() {
    // an IN is exact only if there is a single value we are testing.
    return operator == Operator.IN && arrayValue.size() == 1;
  }

  @Override
  public boolean filterIsSlice() {
    return false;
  }
}
