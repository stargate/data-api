package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.select.OngoingSelection;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.function.Function;

/**
 * Interface for a class that can add the CQL column selection clause to a CQL query built using the
 * Java Driver Query Builder.
 *
 * <p>This is the solum selection part below:
 *
 * <pre>
 * SELECT
 *  column1, column2
 * FROM
 *  MyTable
 * WHERE
 *  columnName = B70DE1D0-9908-4AE3-BE34-5573E5B09F14;
 * </pre>
 *
 * The function should use the {@link OngoingSelection} to add the values to the statement.
 */
public interface SelectCQLClause extends Function<OngoingSelection, Select>, CQLClause {}
