package io.stargate.sgv2.jsonapi.service.operation.query;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdateSetResolver;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdateUnsetResolver;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import java.util.List;

/**
 * CQL Column assignment that set the value of a CQL column.
 *
 * <p>Currently resolved from API operator $set, $unset, see {@link TableUpdateSetResolver}, {@link
 * TableUpdateUnsetResolver}.
 */
public class ColumnSetToAssignment extends ColumnAssignment {

  public ColumnSetToAssignment(CqlNamedValue namedValue) {
    super(namedValue);
  }

  @Override
  public UpdateWithAssignments apply(OngoingAssignment ongoingAssignment, List<Object> objects) {
    addPositionalValues(objects);
    return ongoingAssignment.set(Assignment.setColumn(namedValue.name(), bindMarker()));
  }
}
