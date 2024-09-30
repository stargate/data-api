package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
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

public class InTableFilter extends TableFilter {

  private final List<Object> arrayValue;

  private final Operator operator;

  public enum Operator {
    IN,
//    NIN,
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
        throw ErrorCodeV1.TABLE_COLUMN_UNKNOWN.toApiException(e.getMessage());
      } catch (MissingJSONCodecException e) {
        throw ErrorCodeV1.TABLE_COLUMN_TYPE_UNSUPPORTED.toApiException(e.getMessage());
      } catch (ToCQLCodecException e) {
        // TODO AARON - Handle error
        throw new RuntimeException(e);
      }
    }

    return ongoingWhereClause.where(Relation.column(getPathAsCqlIdentifier()).in(bindMarkers));
  }

  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException(
        "No supported - will be modified when we migrate collections filters java driver");
  }

  /**
   * Analyze the $in API table filter and get corresponding usage info.
   * <P>
   * [$in against scalar columns]
   * Without SAI index, following 14 column types text/int/timestamp/ascii/date/time/timestamp/boolean/varint/tinyint/decimal/smallint/double/bigint/float need ALLOW FILTERING. <br>
   * We can NOT build SAI index on duration column type, so ALLOW FILTERING is also needed.
   * TODO, blob column
   *
   * <P>
   * [$in against collection columns]
   *
   *
   * @param tableSchemaObject tableSchemaObject
   * @return TableFilterAnalyzedUsage
   */
  @Override
  public TableFilterAnalyzedUsage analyze(TableSchemaObject tableSchemaObject) {
    //check if filter is against an existing column
    final ColumnMetadata column = getColumn(tableSchemaObject);

    // if it is against a scalar column
    // For scalar column, 15 types(no blob yet), if no SAI index on the column, need ALLOW FILTERING
    if (!hasSaiIndexOnColumn(tableSchemaObject)) {
      return new TableFilterAnalyzedUsage(
          path, true, Optional.of("ALLOW FILTERING turned on"));
      // TODO, blob
    }

    return new TableFilterAnalyzedUsage(path, false, Optional.empty());

    // if it is against a collection column

  }
}
