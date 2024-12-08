package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/**
 * Holds the paging state, either coming from the user in a request or extracted from a result set.
 *
 * <p>And then can be used to add it to a statement or to get the string representation to put into
 * a {@link io.stargate.sgv2.jsonapi.api.model.command.CommandResult}
 */
public class CqlPagingState {

  // Public because some commands do not have any paging state so they can just get this
  public static final CqlPagingState EMPTY = new CqlPagingState(true, null, null);

  private final boolean isEmpty;
  private final String pagingStateString;
  private final ByteBuffer pagingStateBuffer;

  private CqlPagingState(boolean isEmpty, String pagingStateString, ByteBuffer pagingStateBuffer) {
    this.isEmpty = isEmpty;
    this.pagingStateString = pagingStateString;
    this.pagingStateBuffer = pagingStateBuffer;
  }

  public static CqlPagingState from(String pagingState) {
    return switch (pagingState) {
        // null and empty string are considered empty paging state
        // any non-zero length string is considered page state
        // the same standard is used in collections
      case null -> EMPTY;
      case "" -> EMPTY;
      default -> new CqlPagingState(false, pagingState, decode(pagingState));
    };
  }

  public static CqlPagingState from(AsyncResultSet rSet) {
    return switch (rSet) {
      case AsyncResultSet ars when ars.hasMorePages() -> {
        var pagingBuffer = rSet.getExecutionInfo().getPagingState();
        yield new CqlPagingState(false, encode(pagingBuffer), pagingBuffer);
      }
      case null, default -> EMPTY;
    };
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public SimpleStatement addToStatement(SimpleStatement statement) {
    return isEmpty ? statement : statement.setPagingState(pagingStateBuffer);
  }

  public Optional<String> getPagingStateString() {
    return isEmpty ? Optional.empty() : Optional.of(pagingStateString);
  }

  @Override
  public String toString() {
    return new StringBuilder("CqlPagingState{")
        .append("isEmpty=")
        .append(isEmpty)
        .append(", pagingStateString='")
        .append(pagingStateString)
        .append('\'')
        .append('}')
        .toString();
  }

  private static ByteBuffer decode(String pagingState) {
    return ByteBuffer.wrap(Base64.getDecoder().decode(pagingState));
  }

  private static String encode(ByteBuffer pagingState) {
    Objects.requireNonNull(pagingState, "pagingState must not be null");
    return Base64.getEncoder().encodeToString(pagingState.array());
  }
}
