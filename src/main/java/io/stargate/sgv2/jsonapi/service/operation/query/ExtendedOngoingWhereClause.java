package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;

/**
 * This class help to bind additional information with Driver OnGoingWhereClause
 *
 * <p>AllowFiltering, this method is only implemented by Driver Select Take this where clause as
 * example, WHERE name!='tim', it needs AllowFiltering on with/without SAI index on the column. Data
 * API should do the work and return warning msg.
 *
 * <p>Warning???
 *
 * @param <T>
 */
public class ExtendedOngoingWhereClause<T extends OngoingWhereClause<T>> {

  public final OngoingWhereClause<T> ongoingWhereClause;
  public final boolean allowFiltering;

  public ExtendedOngoingWhereClause(
      OngoingWhereClause<T> ongoingWhereClause, boolean allowFiltering) {
    this.ongoingWhereClause = ongoingWhereClause;
    this.allowFiltering = allowFiltering;
  }

  public ExtendedOngoingWhereClause<T> where(
      Relation whereRelation, boolean shouldAddAllowFiltering) {
    return new ExtendedOngoingWhereClause<>(
        ongoingWhereClause.where(whereRelation), shouldAddAllowFiltering);
  }

  public Select mayApplyAllowFilteringToSelect() {
    Select select = (Select) ongoingWhereClause;
    if (allowFiltering) {
      return select.allowFiltering();
    }
    return select;
  }

  /**
   * For Delete, Data API won't add ALLOW FILTERING In Cassandra, the ALLOW FILTERING option allows
   * you to run queries that involve filtering on non-primary key columns. However, you cannot use
   * ALLOW FILTERING with DELETE or UPDATE operations directly, as it’s primarily designed for
   * SELECT statements.
   */
  public Delete noAllowFilteringToDelete() {
    return (Delete) ongoingWhereClause;
  }

  /**
   * For Update, Data API won't add ALLOW FILTERING In Cassandra, the ALLOW FILTERING option allows
   * you to run queries that involve filtering on non-primary key columns. However, you cannot use
   * ALLOW FILTERING with DELETE or UPDATE operations directly, as it’s primarily designed for
   * SELECT statements.
   */
  public Update noAllowFilteringToUpdate() {
    return (Update) ongoingWhereClause;
  }
}
