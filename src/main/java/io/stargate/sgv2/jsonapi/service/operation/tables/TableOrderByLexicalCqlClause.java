package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.Deferred;
import java.util.List;
import java.util.Objects;

/**
 * A CQL clause that adds an ORDER BY clause to a SELECT statement to BM25 (Lexical) sort.
 *
 * <p>Note: Only supports sorting on a single column
 */
public class TableOrderByLexicalCqlClause implements OrderByCqlClause {

  private final CqlNamedValue sortText;
  private final Integer defaultLimit;

  public TableOrderByLexicalCqlClause(CqlNamedValue sortText, Integer defaultLimit) {
    this.sortText = Objects.requireNonNull(sortText, "sortVector must not be null");
    this.defaultLimit = Objects.requireNonNull(defaultLimit, "defaultLimit must not be null");
  }

  @Override
  public Select apply(Select select) {
    // !!! TODO: proper ORDER BY BM25
    return select.orderBy(sortText.name(), ClusteringOrder.DESC);
    // return select.orderByAnnOf(sortVector.name(), sortVector.cqlVector());
  }

  @Override
  public boolean fullyCoversCommand() {
    return true;
  }

  @Override
  public Integer getDefaultLimit() {
    return defaultLimit;
  }

  @Override
  public List<? extends Deferred> deferred() {
    return new CqlNamedValueContainer(List.of(sortText)).deferredValues();
  }
}
