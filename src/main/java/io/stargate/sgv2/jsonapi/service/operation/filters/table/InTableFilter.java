package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

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
import io.stargate.sgv2.jsonapi.service.operation.query.ExtendedOngoingWhereClause;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import java.util.ArrayList;
import java.util.List;

public class InTableFilter extends TableFilter {

  private final List<Object> arrayValue;

  private final Operator operator;

  public enum Operator {
    IN,
    NIN,
  }

  public InTableFilter(Operator operator, String path, List<Object> arrayValue) {
    super(path);
    this.arrayValue = arrayValue;
    this.operator = operator;
  }

  @Override
  public <StmtT extends OngoingWhereClause<StmtT>> ExtendedOngoingWhereClause<StmtT> apply(
      TableSchemaObject tableSchemaObject,
      ExtendedOngoingWhereClause<StmtT> extendedOngoingWhereClause,
      List<Object> positionalValues) {
    List<Term> bindMarkers = new ArrayList<>();

    // TODO this codec part we won't do in the operation level? refer to insertOne
    //    TasbleInsertValuesContainer
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

    return extendedOngoingWhereClause.where(
        Relation.column(getPathAsCqlIdentifier()).in(bindMarkers),
        shouldAddAllowFiltering(tableSchemaObject));
  }

  /**
   * Example Table definition: {"definition": {"columns": {"id": {"type": "int"},"age": {"type":
   * "int"},"name": {"type": "text"}},"primaryKey": "id"}}.<br>
   * Note, name column has SAI index on it, age column has NO SAI index on it.
   *
   * <p>[API table filter $in on scalar columns] <br>
   * With SAI index, no allow filtering needed. e.g. SELECT name,id,age FROM
   * filter."singlePrimaryKeyTable" WHERE name IN (?,?) LIMIT 1 <br>
   * Without SAI index, no allow filtering needed. e.g. SELECT name,id,age FROM
   * filter."singlePrimaryKeyTable" WHERE age IN (?,?) LIMIT 1 ALLOW FILTERING
   *
   * <p>[API table filter $in on collection column] TODO
   *
   * @param tableSchemaObject tableSchemaObject
   * @return boolean to indicate ALLOW FILTERING is needed or not
   */
  @Override
  public boolean shouldAddAllowFiltering(TableSchemaObject tableSchemaObject) {
    boolean mayAddAllowFiltering = !hasSaiIndexOnColumn(tableSchemaObject);
    return mayAddAllowFiltering;
  }

  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException(
        "No supported - will be modified when we migrate collections filters java driver");
  }
}
