package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters;

import io.stargate.sgv2.jsonapi.service.cql.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.IndexUsage;

import java.util.function.Supplier;

/** Base for the DB filters / conditions that we want to use with queries */
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
  // HACK aaron - referenced from FindOperation, Needs to be fixed
  public String getPath() {
    return path;
  }

}
