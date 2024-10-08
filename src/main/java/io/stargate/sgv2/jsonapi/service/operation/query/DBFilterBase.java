package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import java.util.function.Supplier;

/** Base for the DB filters / conditions that we want to use with queries */

/**
 * DB Filters represent a logical filter operation we want to run against a target collection table.
 *
 * <p>TODO: NOTE: See {@link TableFilter} for how we are doing this for tables so it uses the Java
 * driver query builder, when we migrate away from the old gRPC bridge query builder we will move ot
 * something like that and the {@link CQLClause} interface.
 *
 * <p>
 *
 * <p>They are logical in that they represent the filter the user wants to apply, e.g. a $in filter
 * for an array in a document will use the @link{InFilter} if the target is a Collection
 *
 * <p>The DBFilter builds the BuildConditions which represent the actual CQL query conditions to
 * run.
 */
public abstract class DBFilterBase implements Supplier<BuiltCondition> {

  // TODO: change the supplier to be a list so the IDFilter does not have a special getAll

  /** Filter condition element path. */
  public final String path;

  public final IndexUsage indexUsage;

  private transient CqlIdentifier pathAsIdentifier;

  protected DBFilterBase(String path, IndexUsage indexUsage) {
    this.path = path;
    this.indexUsage = indexUsage;
  }

  /**
   * Returns filter condition element path.
   *
   * @return
   */
  protected String getPath() {
    return path;
  }

  protected CqlIdentifier getPathAsCqlIdentifier() {
    if (pathAsIdentifier == null) {
      pathAsIdentifier = CqlIdentifier.fromInternal(path);
    }
    return pathAsIdentifier;
  }
}
