package io.stargate.sgv3.docsapi.operations;

import io.stargate.sgv3.docsapi.shredding.IDShreddedDocument;
import io.stargate.sgv3.docsapi.shredding.WritableShreddedDocument;
import java.util.List;

/**
 * The internal to modification operation results, what were the ID's of the docs we changed and
 * what change.
 *
 * <p>This is the corresponding class to {@link ReadOperationPage} and only used by subclasses of
 * {@link ModifyOperation}
 */
public class ModifyOperationPage {

  public final List<IDShreddedDocument> insertedIds;
  public final List<IDShreddedDocument> updatedIds;
  public final List<WritableShreddedDocument> insertedDocs;
  public final List<WritableShreddedDocument> updatedDocs;

  private ModifyOperationPage(
      List<IDShreddedDocument> insertedIds,
      List<WritableShreddedDocument> insertedDocs,
      List<IDShreddedDocument> updatedIds,
      List<WritableShreddedDocument> updatedDocs) {
    this.insertedIds = insertedIds;
    this.insertedDocs = insertedDocs;
    this.updatedIds = updatedIds;
    this.updatedDocs = updatedDocs;
  }

  public static ModifyOperationPage from(
      List<String> insertedIds,
      List<WritableShreddedDocument> insertedDocs,
      List<String> updatedIds,
      List<WritableShreddedDocument> updatedDocs) {
    return new ModifyOperationPage(
        IDShreddedDocument.fromIdStrings(insertedIds),
        insertedDocs,
        IDShreddedDocument.fromIdStrings(updatedIds),
        updatedDocs);
  }

  /**
   * These two classes are kind of similar, but the OperationResult is the thing that escapes the
   * Operation layer to the Command layer, while ModifiedDocumentPage does not.
   *
   * @return
   */
  public OperationResult createOperationResult() {
    return OperationResult.builder()
        .withInsertedIds(insertedIds)
        .withUpdatedIds(updatedIds)
        .build();
  }
}
