package io.stargate.sgv3.docsapi.updater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.commands.clauses.UpdateClause;
import io.stargate.sgv3.docsapi.commands.clauses.update.SetUpdateOperation;
import io.stargate.sgv3.docsapi.commands.clauses.update.UpdateClauseOperation;
import io.stargate.sgv3.docsapi.operations.DocumentUpdaterFunction;
import io.stargate.sgv3.docsapi.shredding.ReadableShreddedDocument;
import io.stargate.sgv3.docsapi.shredding.Shredder;
import io.stargate.sgv3.docsapi.shredding.WritableShreddedDocument;
import java.util.ArrayList;
import java.util.List;
import org.javatuples.Pair;

/**
 * Responsible for updating documents for commands like updateOne, findOneAndUpdate.
 *
 * <p>This sits somewhere between commands and operations. We will want to de-shred the docs we have
 * read so we run the updates against the Jackson node data model so we don't corrupt the shredding
 * model with how to do updates. Then re-shred so it is writeable.
 *
 * <p>The {@link Updater}'s simply reference the {@link UpdateClauseOperation} rather than have
 * their own data definition because there is nothing different we need to store. The updater has
 * the behaviour of how to apply the operation to a document, and the clause remains behaviour free.
 *
 * <p>In a package because it will get bigger to handle all of the features for updating documents.
 *
 * <p>NOTE: This class will be responsible for "upserting" documents, creating a new doc if none
 * matched a filter.
 */
public class DocumentUpdater implements DocumentUpdaterFunction {

  private final List<Updater> updaters;
  private boolean upsert;
  private List<? extends ReadableShreddedDocument> originalDocs;

  public DocumentUpdater(UpdateClause clause, boolean upsert) {
    this.upsert = upsert;

    // TODO: upsert not supported yet
    assert !upsert;

    updaters = clause.operations.stream().map(Updater::updatorForOperation).toList();

    assert updaters.size() == clause.operations.size();
  }

  @Override
  public Result updateDocuments(List<? extends ReadableShreddedDocument> originalShredded) {
    this.originalDocs = originalShredded;
    var shredder = new Shredder();

    List<ObjectNode> originalJson =
        shredder.deshred(null, originalShredded).stream()
            .map(jsonNode -> (ObjectNode) jsonNode)
            .toList();

    assert originalJson.size() == originalShredded.size();

    // zip these together because we need the original document to re-shredd so we have the
    // correct txID to match to the document.
    // Build a list of the documents that were updated and their original document.

    // This is easier to read (and prob efficient) using iterators rather than zipping streams and
    // optionals
    var shreddedIter = originalShredded.iterator();
    var jsonIter = originalJson.iterator();
    List<Pair<ReadableShreddedDocument, JsonNode>> modifiedPairs =
        new ArrayList<>(originalShredded.size());

    while (shreddedIter.hasNext() && jsonIter.hasNext()) {
      var nextShredded = shreddedIter.next();
      var nextJson = jsonIter.next();

      // TODO: check these docs have the same ID
      if (Updater.applyAll(updaters, nextJson)) {
        modifiedPairs.add(Pair.with(nextShredded, (JsonNode) nextJson));
      }
    }

    // Now need to re-shred, this will make sure the new shredded documents have the txID of the
    // original
    List<WritableShreddedDocument> modifiedShredded = shredder.reshred(modifiedPairs);

    // No upserted docs yet.
    return new Result(modifiedShredded, List.of());
  }

  public List<? extends ReadableShreddedDocument> getOriginalDocs() {
    return originalDocs;
  }

  /**
   * Behaviour of hte update operations is implemented behind this interface.
   *
   * <p>NOTE to implementors, you must add the class to {@link #updatorForOperation} so it can be
   * found by the {@link DocumentUpdater}
   *
   * <p>TODO: there an possible performance enhancement if we track the paths in the document that
   * are updated, we could then only re-shred the parts that have changed. But that would mean
   * reading the entire row from the db, not just what we need to de-shred the document. Now not
   * just noting the possibility.
   */
  static interface Updater {

    /**
     * Called to update the document.
     *
     * @param doc This is the top level {@link ObjectNode} of the json document.
     * @return {@code True} if the document was updated, false otherwise.
     */
    boolean updateDocument(ObjectNode doc);

    /**
     * Call to map a {@link UpdateClauseOperation} to the {@link Updater} that implements it.
     *
     * @param operation
     * @return
     */
    static Updater updatorForOperation(UpdateClauseOperation operation) {

      if (operation instanceof SetUpdateOperation) {
        return new SetUpdater((SetUpdateOperation) operation);
      }
      throw new RuntimeException(
          String.format("Unknown UpdateClauseOperation class %s", operation));
    }

    /**
     * Applies all of the {@link Updater}s to the document and returns {@code True} if any updater
     * returned true.
     *
     * @param updaters
     * @param doc
     * @return
     */
    static boolean applyAll(List<Updater> updaters, ObjectNode doc) {
      boolean docUpdated = false;
      for (var updater : updaters) {
        docUpdated = updater.updateDocument(doc) || docUpdated;
      }
      return docUpdated;
    }
  }
}
