package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class InsertManyTableIntegrationTest extends AbstractTableIntegrationTestBase {

  static final String TABLE_1PK_1REGULAR = "table" + System.currentTimeMillis();

  static final Map<String, Object> COLUMNS_1PK_1REGULAR =
      Map.ofEntries(
          Map.entry("id", Map.of("type", "text")), Map.entry("name", Map.of("type", "text")));

  static final String SAMPLE_DOCUMENT_JSON_1PK_1REGULAR =
      """
                      {
                          "id": %s,
                          "name": %s
                      }
                  """;

  @BeforeAll
  public final void createDefaultTables() {
    // create table and index all columns.
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(TABLE_1PK_1REGULAR, COLUMNS_1PK_1REGULAR, "id")
        .wasSuccessful();
  }

  private static Stream<Arguments> ORDERED_OR_NOT() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("ORDERED_OR_NOT")
  void successfulInsertManyTables(boolean ordered) throws DocumentException {
    List<String> documents = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      documents.add(
          String.format(
              SAMPLE_DOCUMENT_JSON_1PK_1REGULAR,
              addDoubleQuote("id" + i),
              addDoubleQuote("name" + i)));
    }
    if (ordered) {
      assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
          .templated()
          .insertMany(documents, true)
          .wasSuccessful();
    } else {
      assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
          .templated()
          .insertMany(documents, false)
          .wasSuccessful();
    }
    // verify 10 rows are inserted
    assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
        .postFind("{ \"filter\": { } }")
        .wasSuccessful()
        .hasDocuments(10);
    // truncate the table
    assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
        .templated()
        .deleteMany("{}")
        .wasSuccessful()
        .hasNoErrors();
  }

  @ParameterizedTest
  @MethodSource("ORDERED_OR_NOT")
  void faultyRowInsertManyTables(boolean ordered) throws DocumentException {
    // Note, we have 2nd and 4th document as faulty, since dataType for name is not matching in
    // codec
    List<String> documents = new ArrayList<>();
    documents.add(
        String.format(
            SAMPLE_DOCUMENT_JSON_1PK_1REGULAR,
            addDoubleQuote("id" + 1),
            addDoubleQuote("name" + 1)));
    documents.add(String.format(SAMPLE_DOCUMENT_JSON_1PK_1REGULAR, addDoubleQuote("id" + 2), 123));
    documents.add(
        String.format(
            SAMPLE_DOCUMENT_JSON_1PK_1REGULAR,
            addDoubleQuote("id" + 3),
            addDoubleQuote("name" + 3)));
    documents.add(String.format(SAMPLE_DOCUMENT_JSON_1PK_1REGULAR, addDoubleQuote("id" + 4), 456));
    documents.add(
        String.format(
            SAMPLE_DOCUMENT_JSON_1PK_1REGULAR,
            addDoubleQuote("id" + 5),
            addDoubleQuote("name" + 5)));

    if (ordered) {
      // if ordered, any row before the faulty row should succeed, so just id1 is inserted
      // we should get two errors back for id2 and id4
      assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
          .templated()
          .insertMany(documents, true)
          .hasInsertedIdCount(1)
          .body("errors", org.hamcrest.Matchers.hasSize(2))
          .body(
              "errors[0].errorCode",
              org.hamcrest.Matchers.is(DocumentException.Code.INVALID_COLUMN_VALUES.name()))
          .body(
              "errors[1].errorCode",
              org.hamcrest.Matchers.is(DocumentException.Code.INVALID_COLUMN_VALUES.name()));
      // select against just to make sure
      assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
          .postFind("{ \"filter\": { } }")
          .wasSuccessful()
          .hasDocuments(1);

    } else {
      // if unordered, faulty row does not impact the insertion of other rows, so id1,id3,id5 are
      // inserted
      // we also should get two errors back for id2 and id4
      assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
          .templated()
          .insertMany(documents, false)
          .hasInsertedIdCount(3)
          .body("errors", org.hamcrest.Matchers.hasSize(2))
          .body(
              "errors[0].errorCode",
              org.hamcrest.Matchers.is(DocumentException.Code.INVALID_COLUMN_VALUES.name()))
          .body(
              "errors[1].errorCode",
              org.hamcrest.Matchers.is(DocumentException.Code.INVALID_COLUMN_VALUES.name()));
      assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
          .postFind("{ \"filter\": { } }")
          .wasSuccessful()
          .hasDocuments(3);
    }

    // truncate the table
    assertTableCommand(keyspaceName, TABLE_1PK_1REGULAR)
        .templated()
        .deleteMany("{}")
        .wasSuccessful()
        .hasNoErrors();
  }

  private String addDoubleQuote(Object value) {
    if (value instanceof String) {
      return "\"" + value + "\"";
    }
    return value.toString(); // Return as is for non-string values
  }
}
