package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters;

import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.IndexUsage;
import java.util.function.Supplier;

/** Base for the DB filters / conditions that we want to use with queries */

/**
 * DB Filters represent a logical filter operation we want to run against a target collection table.
 *
 * They are logical in that they represent the filter the user wants to apply, e.g. a $in filter for an array in a
 * document will use the @link{InFilter} if the target is a Collection, and the TODO when the target is a table.
 *
 * The DBFilter builds the BuildConditions which represent the actual CQL query conditions to run.
 */
public abstract class DBFilterBase implements Supplier<BuiltCondition> {

  /** Tracks the index column usage */
  public final IndexUsage indexUsage = new IndexUsage();

  /** Filter condition element path. */
  protected final String path;

  protected DBFilterBase(String path) {
    this.path = path;
  }

  /**
   * Returns filter condition element path.
   *
   * @return
   */
  protected String getPath() {
    return path;
  }
}
