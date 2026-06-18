package io.stargate.sgv2.jsonapi.service.cqldriver.override;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.OrderingClause;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultSelect;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Map;
import java.util.Optional;

/** Subclass for forcing use of "ORDER BY BM25 OF" (lexical sort) in the select clause. */
public class LexicalSortSelect extends DefaultSelect {
  public LexicalSortSelect(DefaultSelect base, CqlIdentifier sortColumn, String sortText) {
    super(
        base.getKeyspace(),
        base.getTable(),
        base.isJson(),
        base.isDistinct(),
        base.getSelectors(),
        base.getRelations(),
        base.getGroupByClauses(),
        Optional.of(new Bm25OrderingClause(sortColumn, sortText)),
        base.getLimit(),
        base.getPerPartitionLimit(),
        base.allowsFiltering());
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
        base.getOrderingClause(),
        limit,
        base.getPerPartitionLimit(),
        allowsFiltering);
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
  public Select withOrderingClause(OrderingClause newOrderingClause) {
    throw new UnsupportedOperationException(
        "LexicalSortSelect does not support withOrderingClause()");
  }

  /**
   * Custom {@link OrderingClause} that renders the lexical sort clause {@code ORDER BY <column>
   * BM25 OF '<text>'}, which the driver's query builder does not support natively.
   */
  private static final class Bm25OrderingClause extends OrderingClause {
    private final CqlIdentifier sortColumn;
    private final String sortText;

    Bm25OrderingClause(CqlIdentifier sortColumn, String sortText) {
      this.sortColumn = sortColumn;
      this.sortText = sortText;
    }

    @Override
    public void appendTo(StringBuilder builder) {
      builder
          .append(" ORDER BY ")
          .append(sortColumn.asCql(false))
          .append(" BM25 OF ")
          // 11-Jul-2025, tatu: ideally would use a BindMarker here, but binding
          //    values is difficult to do from this context. So escape explicitly.
          .append(Strings.quote(sortText));
    }
  }
}
