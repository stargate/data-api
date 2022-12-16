package io.stargate.sgv3.docsapi.operations;

import io.stargate.sgv3.docsapi.shredding.ReadableShreddedDocument;
import java.util.List;

/**
 * The internal to read operation results, holds the documents we got back.
 *
 * <p>This is the corresponding class to {@link ModifyOperationPage} and only used by subclasses of
 * {@link ReadOperation}
 */
public class ReadOperationPage {

  List<ReadableShreddedDocument> docs;
  String pagingState;

  private ReadOperationPage(List<ReadableShreddedDocument> docs, String pagingState) {
    this.docs = docs;
    this.pagingState = pagingState;
  }

  /**
   * Create a page for a single document with no page state
   *
   * @param doc
   * @return
   */
  public static ReadOperationPage from(ReadableShreddedDocument doc) {
    assert doc != null;
    return new ReadOperationPage(List.of(doc), null);
  }

  public static ReadOperationPage from(List<ReadableShreddedDocument> doc, String pagingState) {
    assert doc != null;
    return new ReadOperationPage(doc, pagingState);
  }

  /**
   * Return an empty page
   *
   * @return
   */
  public static ReadOperationPage empty() {
    return new ReadOperationPage(List.of(), null);
  }

  /**
   * These two classes are kind of similar, but the OperationResult is the thing that escapes the
   * Operation layer to the Command layer, while ShreddedDocumentPage does not.
   *
   * @return
   */
  public OperationResult createOperationResult() {
    return OperationResult.builder().withDocs(docs, pagingState).build();
  }
}
