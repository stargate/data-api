package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;

import java.util.function.Function;

/**
 * Called by an operation when it wants the projection to add the columns it will select from the
 * database to the {@link Select} from the Query builder.
 *
 * <p>Implementations should add the columns they need by name from their internal state. The
 * projection should already have been valided as valid to run against the table, all the columns
 * in the projection should exist in the table.
 *
 * <p>TODO: the select param should be a Select type, is only a SelectFrom because that is where
 * the builder has json(), will change to select when we stop doing that. See AllJSONProjection
 *
 * @param select
 * @return
 */
public interface SelectBuilder extends Function<SelectFrom, Select>{
}
