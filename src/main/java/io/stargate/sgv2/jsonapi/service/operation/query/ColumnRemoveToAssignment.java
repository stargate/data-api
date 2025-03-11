package io.stargate.sgv2.jsonapi.service.operation.query;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdatePullAllResolver;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import java.util.List;

/**
 * CQL Column assignment that removes the value to a list or set CQL type.
 *
 * <p>Currently resolved from by API operator $pullAll, see {@link TableUpdatePullAllResolver}.
 */
public class ColumnRemoveToAssignment extends ColumnAssignment {

  public ColumnRemoveToAssignment(CqlNamedValue namedValue) {
    super(namedValue);
  }

  @Override
  public UpdateWithAssignments apply(OngoingAssignment ongoingAssignment, List<Object> objects) {
    addPositionalValues(objects);
    return ongoingAssignment.remove(namedValue.name(), bindMarker());
  }
}
