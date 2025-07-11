package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultSelect;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.LexicalSortSelect;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.Deferred;
import java.util.List;
import java.util.Objects;

/**
 * A CQL clause that adds an ORDER BY clause to a SELECT statement for BM25 (Lexical) sort.
 *
 * <p>Note: Only supports sorting on a single column
 */
public class TableOrderByLexicalCqlClause implements OrderByCqlClause {

  private final CqlNamedValue lexicalSort;
  private final Integer defaultLimit;

  public TableOrderByLexicalCqlClause(CqlNamedValue lexicalSort, Integer defaultLimit) {
    this.lexicalSort = Objects.requireNonNull(lexicalSort, "lexicalSort must not be null");
    this.defaultLimit = Objects.requireNonNull(defaultLimit, "defaultLimit must not be null");
  }

  @Override
  public Select apply(Select select) {
    return new LexicalSortSelect(
        (DefaultSelect) select, lexicalSort.name(), (String) lexicalSort.value());
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
    return new CqlNamedValueContainer(List.of(lexicalSort)).deferredValues();
  }
}
