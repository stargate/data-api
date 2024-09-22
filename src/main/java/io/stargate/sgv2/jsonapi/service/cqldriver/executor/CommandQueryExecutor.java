package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configured to execute queries for a specific command that relies on drive profiles MORE TODO
 * WORDS
 *
 * <p>The following settings should be set via the driver profile:
 *
 * <ul>
 *   <li><code>page-size</code>
 *   <li><code>consistency</code>
 *   <li><code>serial-consistency</code>
 *   <li><code>default-idempotence</code>
 * </ul>
 */
public class CommandQueryExecutor {

  private static final String PROFILE_NAME_SEPARATOR = "-";

  public enum QueryTarget {
    TABLE,
    COLLECTION;

    final String profilePrefix;

    QueryTarget() {
      this.profilePrefix = name().toLowerCase();
    }
  }

  private enum QueryType {
    READ,
    WRITE;

    final String profileSuffix;

    QueryType() {
      this.profileSuffix = name().toLowerCase();
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(CommandQueryExecutor.class);

  private final CQLSessionCache cqlSessionCache;
  private final RequestContext requestContext;
  private final QueryTarget queryTarget;

  public CommandQueryExecutor(
      CQLSessionCache cqlSessionCache, RequestContext requestContext, QueryTarget queryTarget) {
    this.cqlSessionCache =
        Objects.requireNonNull(cqlSessionCache, "cqlSessionCache must not be null");
    this.requestContext = Objects.requireNonNull(requestContext, "requestContext must not be null");
    this.queryTarget = queryTarget;
  }

  private CqlSession session() {
    return cqlSessionCache.getSession(
        requestContext.tenantId().orElse(""), requestContext.authToken().orElse(""));
  }

  private String getExecutionProfile(QueryType queryType) {
    return queryTarget.profilePrefix + PROFILE_NAME_SEPARATOR + queryType.profileSuffix;
  }

  private SimpleStatement withExecutionProfile(SimpleStatement statement, QueryType queryType) {
    return statement.setExecutionProfileName(getExecutionProfile(queryType));
  }

  private Uni<AsyncResultSet> executeAndWrap(SimpleStatement statement) {
    return Uni.createFrom().completionStage(session().executeAsync(statement));
  }

  public Uni<AsyncResultSet> executeRead(SimpleStatement statement) {
    Objects.requireNonNull(statement, "statement must not be null");

    statement = withExecutionProfile(statement, QueryType.READ);
    return executeAndWrap(statement);
  }

  public Uni<AsyncResultSet> executeWrite(SimpleStatement statement) {
    Objects.requireNonNull(statement, "statement must not be null");

    statement = withExecutionProfile(statement, QueryType.WRITE);
    return executeAndWrap(statement);
  }
}
