package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One read that finds candidate documents for a findAndRerank command, the results of all the legs
 * in a {@link FindAndRerankPlan} are merged and reranked.
 *
 * <p>A leg describes the read, it is decided by the {@link FindAndRerankPlanner} and can build that
 * read more than once. The one-shot objects needed to run the read, such as the {@link
 * DeferredVectorize}, are only created when {@link #buildInnerRead(FilterDefinition, boolean,
 * boolean)} is called.
 *
 * <p>NOTE: this interface is sealed so that a switch over the legs must handle every kind of leg.
 * The project has no <code>module-info.java</code>, so all implementations must be in this same
 * package.
 *
 * <p>TODO: GH #2603 - the leg that reads with only the filter must be added to this package and to
 * the permits clause.
 */
public sealed interface RerankLeg extends Recordable permits LexicalLeg, VectorLeg, VectorizeLeg {

  /** The source of the ranks for the documents this leg reads. */
  Rank.RankSource rankSource();

  /** The query to rerank with when the command does not set <code>rerankQuery</code>. */
  Optional<String> defaultRerankQuery();

  /** The passage field to rerank on when the command does not set <code>rerankOn</code>. */
  Optional<String> defaultPassageField();

  /**
   * Builds the inner find command for this leg. Every call returns a new read, because the read and
   * its {@link DeferredVectorize} can only be used once.
   *
   * @param filterDefinition The filter from the findAndRerank command.
   * @param includeScores The <code>includeScores</code> option, only legs that sort by vector ask
   *     the read to include the similarity.
   * @param includeSortVector The <code>includeSortVector</code> option.
   * @return The inner read, with a new {@link DeferredVectorize} when the leg must embed text
   *     before it can read.
   */
  InnerRead buildInnerRead(
      FilterDefinition filterDefinition, boolean includeScores, boolean includeSortVector);

  /**
   * The inner find command for a leg.
   *
   * @param findCommand The find command to run.
   * @param deferredVectorize <code>null</code> when the leg does not need vectorizing, otherwise
   *     the vectorize that updates the sort clause of the find command when the vector is ready.
   */
  record InnerRead(FindCommand findCommand, DeferredVectorize deferredVectorize) {

    // Need a Projection for the inner finds, it's too complicated to merge the user projection
    // and what we need, because the user may be running a projection to hide fields, which
    // cannot then include fields, so we just use the * wildcard to include all fields for now
    private static final JsonNode INCLUDE_ALL_PROJECTION =
        JsonNodeFactory.instance.objectNode().put("*", 1);

    public InnerRead {
      Objects.requireNonNull(findCommand, "findCommand must not be null");
    }

    /**
     * The deferrables that must be completed before this read can run: the {@link
     * DeferredVectorize} when there is one, otherwise none.
     */
    public List<Deferrable> deferrables() {
      return deferredVectorize == null ? List.of() : List.of(deferredVectorize);
    }

    /**
     * Creates the read, the sort clause is wrapped as is, so a {@link DeferredVectorize} holding
     * the same clause can update it.
     */
    static InnerRead create(
        FilterDefinition filterDefinition,
        SortClause sortClause,
        int readLimit,
        boolean includeSimilarity,
        boolean includeSortVector,
        DeferredVectorize deferredVectorize) {

      // a null clause would be wrapped as an empty sort, and the read would run unsorted
      Objects.requireNonNull(sortClause, "sortClause must not be null");

      var findCommand =
          new FindCommand(
              filterDefinition,
              INCLUDE_ALL_PROJECTION,
              SortDefinition.wrap(sortClause),
              new FindCommand.Options(readLimit, 0, null, includeSimilarity, includeSortVector));
      return new InnerRead(findCommand, deferredVectorize);
    }
  }
}
