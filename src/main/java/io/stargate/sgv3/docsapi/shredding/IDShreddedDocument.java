package io.stargate.sgv3.docsapi.shredding;

import java.util.List;

/**
 * Base for all shredded documents.
 *
 * <p>"Shredded document" covers all ways we represent a document that is not JSON. That could be
 * like this class, we just have an id. Or the {@link ReadableShreddedDocument} which is all we need
 * when reading, or the {@link WritableShreddedDocument} which is everything we need to insert /
 * update.
 *
 * <p>A use for this could be in the list of documents inserted.
 */
public class IDShreddedDocument {

  public final String id;

  public IDShreddedDocument(String id) {
    this.id = id;
  }

  public static List<String> toIdStrings(List<? extends IDShreddedDocument> docs) {
    return docs.stream().map(d -> d.id).toList();
  }

  public static List<IDShreddedDocument> fromIdStrings(List<String> ids) {
    return ids.stream().map(IDShreddedDocument::new).toList();
  }
}
