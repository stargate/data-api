package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.service.shredding.*;
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
 *
 * <p>Supports {@link Deferrable} so that the values needed vectorizing can be deferred until
 * execution time. See {@link #deferred()} for docs.
 */
public abstract class ColumnAssignment implements CQLAssignment, Deferrable {

  protected final CqlNamedValue namedValue;

  /**
   * Create a new instance of the class to set the {@code column} to the {@code value} in the
   * specified {@code tableMetadata}.
   */
  protected ColumnAssignment(CqlNamedValue namedValue) {
    this.namedValue = Objects.requireNonNull(namedValue, "namedValue cannot be null");
  }

  public CqlIdentifier name() {
    return namedValue.name();
  }

  @VisibleForTesting
  public CqlNamedValue namedValue() {
    return namedValue;
  }

  //  public CqlIdentifier name() {
  //    return namedValue.name();
  //  }
  //
  //  public CqlNamedValue namedValue() {
  //    return namedValue;
  //  }

  //  @Override
  //  public UpdateWithAssignments apply(
  //      OngoingAssignment ongoingAssignment, List<Object> positionalValues) {
  //
  //    addPositionalValues(positionalValues);
  //    return updateToAssignment.apply(ongoingAssignment, column);
  //  }

  /**
   * Get the {@link Assignment} for the column and value.
   *
   * <p>Is a separate method to support expansion for collections etc in subtypes.
   *
   * @return
   */
  //  protected Assignment getAssignment() {
  //    return Assignment.setColumn(namedValue.name(), bindMarker());
  //  }

  /**
   * Add the value to the list of positional values to bind to the query.
   *
   * <p>Is a separate method to support expansion for collections etc in subtypes.
   *
   * @param positionalValues
   */
  protected void addPositionalValues(List<Object> positionalValues) {
    positionalValues.add(namedValue.value());
  }

  @Override
  public List<? extends Deferred> deferred() {
    return new CqlNamedValueContainer(List.of(namedValue)).deferredValues();
  }

  /** This method is used for unit test in TableUpdateOperatorTest */
  //    public boolean testEquals(UpdateOperator updateOperator, JsonLiteral<?> value) {
  //      if (updateToAssignment instanceof ColumnAppendToAssignment
  //          && updateOperator == UpdateOperator.PUSH) {
  //        return this.value.equals(value);
  //      } else if (updateToAssignment instanceof ColumnRemoveToAssignment
  //          && updateOperator == UpdateOperator.PULL_ALL) {
  //        return this.value.equals(value);
  //      } else if (updateToAssignment instanceof ColumnSetToAssignment
  //          && (updateOperator == UpdateOperator.SET || updateOperator == UpdateOperator.UNSET)) {
  //        return this.value.equals(value);
  //      }
  //      return false;
  //    }
}
