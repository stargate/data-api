package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.service.cql.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.cql.builder.Predicate;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.IndexUsage;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.JsonTerm;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.util.*;
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

  /**
   * @param hasher
   * @param path Path value is prefixed to the hash value of arrays.
   * @param arrayValue
   * @return
   */
  protected static String getHashValue(DocValueHasher hasher, String path, Object arrayValue) {
    return path + " " + getHash(hasher, arrayValue);
  }

  protected static String getHash(DocValueHasher hasher, Object arrayValue) {
    return hasher.getHash(arrayValue).hash();
  }
}
