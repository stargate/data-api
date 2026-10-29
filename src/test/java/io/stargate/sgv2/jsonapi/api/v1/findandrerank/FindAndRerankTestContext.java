package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.restassured.RestAssured.given;

import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.GeneralResource;
import io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.Fixture;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * What the findAndRerank test interfaces need from the test class that runs them, plus request
 * helpers. {@code FindAndRerankFakeRerankerIntegrationTest} implements this interface and the
 * {@code FindAndRerank*Cases} interfaces, whose {@code default} test methods JUnit runs as methods
 * of that class. So every {@code default} test method name must be unique across the interfaces
 * (failsafe reports and selects tests by name), and helper methods in a {@code *Cases} interface
 * must be {@code private} or {@code static} (a {@code default} helper with the same signature in
 * two interfaces would not compile).
 *
 * <p>A parameterized test uses {@code ParameterizedTest(name = "{0}")}, {@code
 * Arguments.of(Named.of("description", firstValue), ...)} and a {@code static} factory in the same
 * interface, named in full in {@code MethodSource}. A {@code Named} description is shown exactly as
 * written, while a plain {@code String} is shown in quotes with its own quotes escaped. The
 * failsafe report names a case only by method and index; {@link #resetFakeReranker(TestInfo)}
 * prints the description into the case's output.
 *
 * <p>Before each test the fake reranker is cleared; after each test it must have answered and
 * recognized every request. The {@code post*} methods send JSON with RestAssured to the application
 * port and do not check the status code.
 */
public interface FindAndRerankTestContext {

  /** The keyspace of the test class. */
  String keyspace();

  /** True when the backend supports lexical search (HCD), false on DSE. */
  boolean lexicalAvailable();

  /** The HTTP port of the application. */
  int port();

  /** The names of the fixtures this test class has created; a per-instance mutable set. */
  Set<String> createdFixtures();

  /** Creates the fixture on first use (see {@link FindAndRerankFixtures}) and returns its name. */
  default String ensure(Fixture fixture) {
    return FindAndRerankFixtures.ensure(this, fixture, createdFixtures());
  }

  /** Clears the fake reranker and prints the test name and display name (see the class comment). */
  @BeforeEach
  default void resetFakeReranker(TestInfo testInfo) {
    System.out.printf(
        "findAndRerank test case: %s [%s]%n",
        testInfo.getTestMethod().map(Method::getName).orElse("?"), testInfo.getDisplayName());
    FakeRerankerClient.reset();
  }

  /** Waits for delayed replies, so they cannot overlap the next test, then checks the requests. */
  @AfterEach
  default void checkNoUnrecognizedRerankerRequests() {
    FakeRerankerClient.awaitIdle();
    RerankerAssertions.assertNoUnrecognizedRequests();
  }

  /** The headers of the {@code post*} methods that take none. */
  default Map<String, Object> defaultHeaders() {
    return FindAndRerankRequests.headers();
  }

  /** Creates the fixture if needed and posts a command to it with the default headers. */
  default ValidatableResponse postToFixture(Fixture fixture, String commandJson) {
    return postToCollection(ensure(fixture), commandJson);
  }

  /** Creates the fixture if needed and posts a command to it with exactly these headers. */
  default ValidatableResponse postToFixture(
      Fixture fixture, String commandJson, Map<String, ?> headers) {
    return postToCollection(ensure(fixture), commandJson, headers);
  }

  default ValidatableResponse postToCollection(String collection, String commandJson) {
    return postToCollection(collection, commandJson, defaultHeaders());
  }

  /** Posts to the collection (or table) with exactly these headers. */
  default ValidatableResponse postToCollection(
      String collection, String commandJson, Map<String, ?> headers) {
    return post(CollectionResource.BASE_PATH, commandJson, headers, keyspace(), collection);
  }

  default ValidatableResponse postToKeyspace(String commandJson) {
    return postToKeyspace(commandJson, defaultHeaders());
  }

  default ValidatableResponse postToKeyspace(String commandJson, Map<String, ?> headers) {
    return post(KeyspaceResource.BASE_PATH, commandJson, headers, keyspace());
  }

  /** Posts a database level command, for example findRerankingProviders. */
  default ValidatableResponse postToDatabase(String commandJson, Map<String, ?> headers) {
    return post(GeneralResource.BASE_PATH, commandJson, headers);
  }

  /** Posts JSON to a path template such as {@code /v1/{keyspace}/{collection}}. */
  default ValidatableResponse post(
      String pathTemplate, String json, Map<String, ?> headers, Object... pathParams) {
    return given()
        .port(port())
        .headers(headers)
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(pathTemplate, pathParams)
        .then();
  }
}
