package io.stargate.sgv3.docsapi.operations;

import io.stargate.sgv3.docsapi.shredding.ReadableShreddedDocument;
import io.stargate.sgv3.docsapi.shredding.WritableShreddedDocument;
import java.util.List;

/**
 * Interface to abstract the concept of updating documents so the operations tier can just define
 * how a document updater should operation without knowing anything about shredding or how to do it.
 */
public interface DocumentUpdaterFunction {
  Result updateDocuments(List<? extends ReadableShreddedDocument> originalShredded);

  public record Result(
      List<WritableShreddedDocument> modified, List<WritableShreddedDocument> upserted) {}
}
