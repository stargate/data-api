package io.stargate.sgv2.jsonapi.service.cqldriver.override;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultSelect;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;

/** Subclass for forcing use of "ORDER BY BM25 OF" (lexical sort) in the select clause. */
public class LexicalSortSelect extends DefaultSelect {
  private static final ImmutableMap<CqlIdentifier, ClusteringOrder> ORDER_BY_PLACEHOLDER =
      ImmutableMap.of(CqlIdentifier.fromCql("abc"), ClusteringOrder.DESC);
  private static final String ORDER_BY_PLACEHOLDER_TEXT = "ORDER BY abc DESC";

  private final CqlIdentifier sortColumn;
  private final String sortText;

  public LexicalSortSelect(DefaultSelect base, CqlIdentifier sortColumn, String sortText) {
    super(
        base.getKeyspace(),
        base.getTable(),
        base.isJson(),
        base.isDistinct(),
        base.getSelectors(),
        base.getRelations(),
        base.getGroupByClauses(),
        // important: use placeholder for ORDER BY
        ORDER_BY_PLACEHOLDER,
        base.getAnn(),
        base.getLimit(),
        base.getPerPartitionLimit(),
        base.allowsFiltering());
    this.sortColumn = sortColumn;
    this.sortText = sortText;
  }

  private LexicalSortSelect(LexicalSortSelect base, Object limit, boolean allowsFiltering) {
    super(
        base.getKeyspace(),
        base.getTable(),
        base.isJson(),
        base.isDistinct(),
        base.getSelectors(),
        base.getRelations(),
        base.getGroupByClauses(),
        base.getOrderings(),
        base.getAnn(),
        limit,
        base.getPerPartitionLimit(),
        allowsFiltering);

    this.sortColumn = base.sortColumn;
    this.sortText = base.sortText;
  }

  // First needed and supported overrides:

  @Override
  public Select allowFiltering() {
    return new LexicalSortSelect(this, this.getLimit(), true);
  }

  @Override
  public Select limit(int limit) {
    return new LexicalSortSelect(this, limit, this.allowsFiltering());
  }

  @Override
  public Select limit(@Nullable BindMarker bindMarker) {
    return new LexicalSortSelect(this, bindMarker, this.allowsFiltering());
  }

  // Then overrides we should not need but want to avoid getting called:

  @Override
  public SelectFrom json() {
    throw new UnsupportedOperationException("LexicalSortSelect does not support json()");
  }

  @Override
  public SelectFrom distinct() {
    throw new UnsupportedOperationException("LexicalSortSelect does not support distinct()");
  }

  @Override
  public Select as(@NonNull CqlIdentifier alias) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support as()");
  }

  @Override
  public Select selector(@NonNull Selector selector) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support selector()");
  }

  @Override
  public Select selectors(@NonNull Iterable<Selector> additionalSelectors) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support selectors()");
  }

  @Override
  public Select withSelectors(@NonNull ImmutableList<Selector> newSelectors) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support withSelectors()");
  }

  @Override
  public Select where(@NonNull Relation relation) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support where()");
  }

  @Override
  public Select where(@NonNull Iterable<Relation> additionalRelations) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support where()");
  }

  @Override
  public Select withRelations(@NonNull ImmutableList<Relation> newRelations) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support withRelations()");
  }

  @Override
  public Select groupBy(@NonNull Selector groupByClause) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support groupBy()");
  }

  @Override
  public Select groupBy(@NonNull Iterable<Selector> newGroupByClauses) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support groupBy()");
  }

  @Override
  public Select withGroupByClauses(@NonNull ImmutableList<Selector> newGroupByClauses) {
    throw new UnsupportedOperationException(
        "LexicalSortSelect does not support withGroupByClauses()");
  }

  @Override
  public Select orderBy(@NonNull CqlIdentifier columnId, @NonNull ClusteringOrder order) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support orderBy()");
  }

  @Override
  public Select orderByAnnOf(@NonNull String columnName, @NonNull CqlVector<?> ann) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support orderByAnnOf()");
  }

  @Override
  public Select orderByAnnOf(@NonNull CqlIdentifier columnId, @NonNull CqlVector<?> ann) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support orderByAnnOf()");
  }

  @Override
  public Select orderByIds(@NonNull Map<CqlIdentifier, ClusteringOrder> newOrderings) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support orderByIds()");
  }

  @Override
  public Select withOrderings(@NonNull ImmutableMap<CqlIdentifier, ClusteringOrder> newOrderings) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support withOrderings()");
  }

  // And finally the override for asCql() to force the ORDER BY clause:

  @Override
  public String asCql() {
    String cql = super.asCql();
    int ix = cql.indexOf(ORDER_BY_PLACEHOLDER_TEXT);
    // Sanity check, should always be there
    if (ix < 0) {
      throw new IllegalStateException(
          "Expected ORDER BY placeholder text ('"
              + ORDER_BY_PLACEHOLDER_TEXT
              + "') not found in CQL: "
              + cql);
    }
    // Force the use of "ORDER BY BM25 OF" for lexical sort
    return cql.substring(0, ix)
        + " ORDER BY "
        + sortColumn.asCql(false)
        + " BM25 OF "
        + QueryBuilder.literal(sortText)
        + cql.substring(ix + ORDER_BY_PLACEHOLDER_TEXT.length());
  }
}
