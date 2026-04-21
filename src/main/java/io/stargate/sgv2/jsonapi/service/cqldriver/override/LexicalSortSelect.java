package io.stargate.sgv2.jsonapi.service.cqldriver.override;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultSelect;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;

/** Subclass for forcing use of "ORDER BY BM25 OF" (lexical sort) in the select clause. */
public class LexicalSortSelect extends DefaultSelect {
  private static final ImmutableMap<CqlIdentifier, ClusteringOrder> ORDER_BY_PLACEHOLDER =
      ImmutableMap.of(CqlIdentifier.fromInternal("abc"), ClusteringOrder.DESC);
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
  public Select limit(BindMarker bindMarker) {
    return new LexicalSortSelect(this, bindMarker, this.allowsFiltering());
  }

  // Then overrides we should not need and want to fail on if called (to avoid
  // obscure bugs if called and superclass instance gets created):

  @Override
  public SelectFrom json() {
    throw new UnsupportedOperationException("LexicalSortSelect does not support json()");
  }

  @Override
  public SelectFrom distinct() {
    throw new UnsupportedOperationException("LexicalSortSelect does not support distinct()");
  }

  @Override
  public Select as(CqlIdentifier alias) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support as()");
  }

  @Override
  public Select selector(Selector selector) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support selector()");
  }

  @Override
  public Select selectors(Iterable<Selector> additionalSelectors) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support selectors()");
  }

  @Override
  public Select withSelectors(ImmutableList<Selector> newSelectors) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support withSelectors()");
  }

  @Override
  public Select where(Relation relation) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support where()");
  }

  @Override
  public Select where(Iterable<Relation> additionalRelations) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support where()");
  }

  @Override
  public Select withRelations(ImmutableList<Relation> newRelations) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support withRelations()");
  }

  @Override
  public Select groupBy(Selector groupByClause) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support groupBy()");
  }

  @Override
  public Select groupBy(Iterable<Selector> newGroupByClauses) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support groupBy()");
  }

  @Override
  public Select withGroupByClauses(ImmutableList<Selector> newGroupByClauses) {
    throw new UnsupportedOperationException(
        "LexicalSortSelect does not support withGroupByClauses()");
  }

  @Override
  public Select orderBy(CqlIdentifier columnId, ClusteringOrder order) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support orderBy()");
  }

  @Override
  public Select orderByAnnOf(String columnName, CqlVector<?> ann) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support orderByAnnOf()");
  }

  @Override
  public Select orderByAnnOf(CqlIdentifier columnId, CqlVector<?> ann) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support orderByAnnOf()");
  }

  @Override
  public Select orderByIds(Map<CqlIdentifier, ClusteringOrder> newOrderings) {
    throw new UnsupportedOperationException("LexicalSortSelect does not support orderByIds()");
  }

  @Override
  public Select withOrderings(ImmutableMap<CqlIdentifier, ClusteringOrder> newOrderings) {
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
        // 11-Jul-2025, tatu: ideally would use a BindMarker here, but binding
        //    values is difficult to do from this context. So escape explicitly.
        + Strings.quote(sortText)
        + cql.substring(ix + ORDER_BY_PLACEHOLDER_TEXT.length());
  }
}
