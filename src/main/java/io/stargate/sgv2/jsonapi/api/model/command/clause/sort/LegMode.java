package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.HYBRID_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.LEXICAL_CONTENT_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import java.util.Arrays;

/**
 * The kind of sort the user wrote in a findAndRerank command.
 *
 * <p>The mode is decided when the sort is parsed, and only from which keys are in the sort. It is
 * the sort the user asked for, not the reads (legs) that will run: the planner combines the mode
 * with the sort values and the collection schema to decide the legs. For example, <code>
 * {"$hybrid": "cheese"}</code> is always {@link #HYBRID}, it runs a vectorize leg and a lexical leg
 * when the collection has a lexical index, and only the vectorize leg when it does not.
 *
 * <p>Only {@link #HYBRID} can be run for now, the planner rejects every other mode with
 * UNSUPPORTED_FIND_AND_RERANK_SORT.
 *
 * <p>TODO: GH #2602 - the planner does not build legs for {@link #VECTORIZE}, {@link #VECTOR} or
 * {@link #LEXICAL} yet, it rejects them until findAndRerank supports single leg sorts.
 */
public enum LegMode {
  /**
   * A <code>$hybrid</code> sort. The planner always runs a vector or vectorize leg, and also a
   * lexical leg when the sort has a <code>$lexical</code> value, or has <code>$hybrid</code> text
   * and the collection has a lexical index.
   */
  HYBRID(HYBRID_FIELD, CommandFeature.HYBRID),
  /** A <code>$vectorize</code> sort, runs one vectorize leg. */
  VECTORIZE(VECTOR_EMBEDDING_TEXT_FIELD, CommandFeature.VECTORIZE),
  /** A <code>$vector</code> sort, runs one vector leg. */
  VECTOR(VECTOR_EMBEDDING_FIELD, CommandFeature.VECTOR),
  /** A <code>$lexical</code> sort, runs one lexical leg. */
  LEXICAL(LEXICAL_CONTENT_FIELD, CommandFeature.LEXICAL),
  /**
   * No sort, or an empty sort. The planner rejects it with UNSUPPORTED_FIND_AND_RERANK_SORT until
   * findAndRerank can run a read with only the filter (GH #2603).
   */
  FILTER(null, null);

  private final String sortField;
  private final CommandFeature commandFeature;

  LegMode(String sortField, CommandFeature commandFeature) {
    this.sortField = sortField;
    this.commandFeature = commandFeature;
  }

  /**
   * The sort field for this mode, e.g. <code>$vectorize</code>, <code>null</code> for {@link
   * #FILTER} which has no field.
   */
  public String sortField() {
    return sortField;
  }

  /** The metrics feature for this mode's field, <code>null</code> for {@link #FILTER}. */
  public CommandFeature commandFeature() {
    return commandFeature;
  }

  /**
   * The mode for a sort field.
   *
   * @param sortField A field from the sort clause, e.g. <code>$lexical</code>.
   * @throws IllegalArgumentException If no mode has this field.
   */
  public static LegMode fromSortField(String sortField) {
    return Arrays.stream(values())
        .filter(mode -> mode.sortField != null && mode.sortField.equals(sortField))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("unknown sort field: " + sortField));
  }
}
