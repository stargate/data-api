package io.stargate.sgv2.jsonapi.service.operation.query;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import java.util.List;
import java.util.Objects;

/**
 * Assigns a single column a value in a CQL Update statement build with the Java Driver Query
 * Builder.
 *
 * <p>NOTE: This class is designed to set scalar column values, basic strings, ints etc. It should
 * be possible to extend it to support more exotic types like collections and UDT's using the
 * appropriate methods on the {@link OngoingAssignment}.
 *
 * <p>Designed to be used with the {@link UpdateValuesCQLClause} to build the full clause.
 */
public class ColumnAssignment implements CQLAssignment {

  private final TableMetadata tableMetadata;
  public final CqlIdentifier column;
  private final JsonLiteral<?> value;

  /**
   * Create a new instance of the class to set the {@code column} to the {@code value} in the
   * specified {@code tableMetadata}.
   *
   * @param tableMetadata The {@link TableMetadata} for the target table.
   * @param column The name of the column to set.
   * @param value the {@link JsonLiteral} value created by shredding the value from the update
   *     clause in the request.
   */
  public ColumnAssignment(TableMetadata tableMetadata, CqlIdentifier column, JsonLiteral<?> value) {
    this.tableMetadata = Objects.requireNonNull(tableMetadata, "tableMetadata cannot be null");
    this.column = Objects.requireNonNull(column, "column cannot be null");
    // Value may be null, this is how to clear a column in CQL
    this.value = value;
  }

  @Override
  public UpdateWithAssignments apply(
      OngoingAssignment ongoingAssignment, List<Object> positionalValues) {

    addPositionalValues(positionalValues);
    return ongoingAssignment.set(getAssignment());
  }

  /**
   * Get the {@link Assignment} for the column and value.
   *
   * <p>Is a separate method to support expansion for collections etc in subtypes.
   *
   * @return
   */
  protected Assignment getAssignment() {
    return Assignment.setColumn(column, bindMarker());
  }

  /**
   * Add the value to the list of positional values to bind to the query.
   *
   * <p>Is a separate method to support expansion for collections etc in subtypes.
   *
   * @param positionalValues
   */
  protected void addPositionalValues(List<Object> positionalValues) {

    var rawValue = value.value();
    try {
      positionalValues.add(
          JSONCodecRegistries.DEFAULT_REGISTRY
              .codecToCQL(tableMetadata, column, rawValue)
              .toCQL(rawValue));
    } catch (MissingJSONCodecException e) {
      // TODO: Better error handling
      throw new RuntimeException(e);
    } catch (UnknownColumnException e) {
      // TODO: Better error handling
      throw new RuntimeException(e);
    } catch (ToCQLCodecException e) {
      throw new RuntimeException(e);
    }
  }
}
