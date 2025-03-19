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
 * <p>Subclasses need to implement {@link #apply(Object, Object)} with the {@link OngoingAssignment}
 * to set the appropriate CQL value for the column, then use {@link #addPositionalValues(List)} to
 * add the values to the list of positional values to bind to the query.
 *
 * <p>Designed to be used with the {@link UpdateValuesCQLClause} to build the full clause.
 *
 * <p>Supports {@link Deferrable} so that the values needed vectorizing can be deferred until
 * execution time. See {@link #deferred()} for docs.
 */
public abstract class ColumnAssignment implements CQLAssignment, Deferrable {

  protected final CqlNamedValue namedValue;

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
}
