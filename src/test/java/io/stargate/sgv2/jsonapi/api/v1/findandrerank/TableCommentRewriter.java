package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils;
import io.stargate.sgv2.jsonapi.testresource.StargateTestResource;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Reads and rewrites a collection's table comment with CQL, to put a collection into states the API
 * cannot create (for example a rerank model that is now END_OF_LIFE). Tests that use it depend on
 * the internal comment format {@code {"collection": {"name": ..., "schema_version": ..., "options":
 * {...}}}}. The application drops a cached schema only when the driver reports a table change, so
 * {@link #withRewrittenComment} polls a probe request, but only until its state differs from the
 * state before the rewrite; the caller's checks then assert the expected result on that response
 * directly. A {@code finally} block restores the original comment and polls until the state is back
 * to what it was.
 *
 * <p>The DSE driver leaves table options, and so the comment, out when it compares old and new
 * table metadata, so a comment change alone would stay invisible until the cache entry expires
 * after 5 minutes. Every comment write therefore also adds a marker column, or drops it when one is
 * there: both drivers compare the columns, and the application still treats a table with an extra
 * column as a collection. Each added marker gets a new name, so a dropped column is never re-added.
 *
 * <p>The CQL session connects like {@code AbstractKeyspaceIntegrationTestBase}: localhost, the
 * mapped CQL port from the test resource, user cassandra.
 */
public final class TableCommentRewriter {

  /** Name prefix of the marker column that every comment write adds or drops. */
  private static final String MARKER_PREFIX = "test_schema_refresh_";

  private static final AtomicInteger MARKER_COUNT = new AtomicInteger();

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static CqlSession session;
  private static int sessionPort;

  private TableCommentRewriter() {}

  /** Returns the comment of the table as stored in {@code system_schema.tables}. */
  public static String readComment(String keyspace, String table) {
    List<Row> rows = schemaRows("comment", "tables", keyspace, table);
    if (rows.isEmpty()) {
      throw new IllegalStateException("No table " + keyspace + "." + table);
    }
    return rows.get(0).getString("comment");
  }

  /**
   * Runs {@code checks} on the first {@code probe} response that shows the comment rewritten by
   * {@code edit}, then restores the comment. Before the rewrite it sends {@code probe} once and
   * keeps the {@code state} of that response, usually {@link #errorCode}; pick a probe whose state
   * differs between the two comments. After the rewrite it polls only until the state differs, so
   * {@code checks} must assert the expected result itself, and a wrong result fails there at once
   * with the expected and actual values. A {@code finally} block writes the original comment back
   * and polls until the state is the original one again. The probe runs many times, so checks that
   * count reranker requests should reset the fake reranker first.
   */
  public static void withRewrittenComment(
      String keyspace,
      String table,
      Consumer<ObjectNode> edit,
      Supplier<ValidatableResponse> probe,
      Function<ValidatableResponse, ?> state,
      Consumer<ValidatableResponse> checks) {
    Object before = state.apply(probe.get());
    String original = rewrite(keyspace, table, edit);
    try {
      checks.accept(
          awaitUntil(
              probe,
              r -> !Objects.equals(state.apply(r), before),
              "the rewritten comment in effect (a state other than " + before + ")"));
    } finally {
      writeComment(keyspace, table, original);
      awaitUntil(
          probe,
          r -> Objects.equals(state.apply(r), before),
          "the original comment in effect again (the state " + before + ")");
    }
  }

  /** The {@code errorCode} of the first error in the response, or null when it has no errors. */
  public static Object errorCode(ValidatableResponse response) {
    return response.extract().path("errors[0].errorCode");
  }

  /**
   * Replaces the comment of the table, then drops the marker column if there is one and adds a new
   * one otherwise, so that the application reloads the schema (see the class comment).
   */
  private static void writeComment(String keyspace, String table, String comment) {
    String qualified = "\"%s\".\"%s\"".formatted(keyspace, table);
    session()
        .execute(
            "ALTER TABLE %s WITH comment = '%s'".formatted(qualified, comment.replace("'", "''")));
    Optional<String> marker =
        schemaRows("column_name", "columns", keyspace, table).stream()
            .map(row -> row.getString("column_name"))
            .filter(name -> name.startsWith(MARKER_PREFIX))
            .findFirst();
    session()
        .execute(
            marker.isPresent()
                ? "ALTER TABLE %s DROP %s".formatted(qualified, marker.get())
                : "ALTER TABLE %s ADD %s%d boolean"
                    .formatted(qualified, MARKER_PREFIX, MARKER_COUNT.incrementAndGet()));
  }

  /** Parses the comment, lets {@code edit} change it, writes it back, and returns the original. */
  private static String rewrite(String keyspace, String table, Consumer<ObjectNode> edit) {
    String original = readComment(keyspace, table);
    try {
      ObjectNode comment = (ObjectNode) MAPPER.readTree(original);
      edit.accept(comment);
      writeComment(keyspace, table, MAPPER.writeValueAsString(comment));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Comment is not JSON: " + original, e);
    }
    return original;
  }

  /**
   * Sends {@code probe} every 250 ms until {@code done} accepts the response, for at most 30 s, and
   * returns that response. When time runs out it fails with {@code awaited}, which says what the
   * wait was for, and the last response body.
   */
  private static ValidatableResponse awaitUntil(
      Supplier<ValidatableResponse> probe, Predicate<ValidatableResponse> done, String awaited) {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    ValidatableResponse last = probe.get();
    while (!done.test(last)) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError(
            "No response in 30 s showed %s; last response: %s"
                .formatted(awaited, last.extract().asString()));
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
      last = probe.get();
    }
    return last;
  }

  /** Reads {@code column} of the rows of {@code system_schema.<schemaTable>} for this table. */
  private static List<Row> schemaRows(
      String column, String schemaTable, String keyspace, String table) {
    String cql = "SELECT %s FROM system_schema.%s WHERE keyspace_name = ? AND table_name = ?";
    return session()
        .execute(SimpleStatement.newInstance(cql.formatted(column, schemaTable), keyspace, table))
        .all();
  }

  private static synchronized CqlSession session() {
    int port = Integer.getInteger(IntegrationTestUtils.CASSANDRA_CQL_PORT_PROP);
    if (session == null || port != sessionPort) {
      if (session != null) {
        session.close();
      }
      boolean dseOrHcd = StargateTestResource.isDse() || StargateTestResource.isHcd();
      session =
          CqlSession.builder()
              .withLocalDatacenter(dseOrHcd ? "dc1" : "datacenter1")
              .addContactPoint(new InetSocketAddress("localhost", port))
              .withAuthCredentials("cassandra", "cassandra")
              .build();
      sessionPort = port;
    }
    return session;
  }
}
