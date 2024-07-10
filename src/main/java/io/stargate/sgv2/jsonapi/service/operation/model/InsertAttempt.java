package io.stargate.sgv2.jsonapi.service.operation.model;

import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import java.util.Optional;

/**
 * Container for an individual Document or Row insertion attempy.
 *
 * <p>Tracks the original input position; document (if available), its id (if available) and
 * possible processing error.
 *
 * <p>Information will be needed to build responses, including the optional detail response see
 * {@link InsertOperationPage}
 *
 * <p>Is {@link Comparable} so that the attempts can be re-sorted into the order provided in the
 * user request, compares based on the {@link #position()}
 */
public interface InsertAttempt extends Comparable<InsertAttempt> {

  /**
   * The zero based position of the document or row in the request from the user.
   *
   * @return integer position
   */
  int position();

  /**
   * The document _id or the row primary key, if known, used to build the response that includes the
   * Id's of the documents / rows that were successfully inserted or failed.
   *
   * <p>Optional as there may be times when the input document / row could not be parsed to get the
   * ID. And separate to having the doc / row shreddded because we may have the id (such as when
   * creating a new document _id sever side) but were not able to shred the document / row.
   *
   * @return The {@link DocRowIdentifer} that identifies the document or row by ID
   */
  Optional<DocRowIdentifer> docRowID();

  /**
   * The first error that happened trying to run this insert.
   *
   * @return
   */
  Optional<Throwable> failure();

  /**
   * Updates the attempt with an error that happened when trying to process the insert.
   *
   * <p>Implmentations must only remember the first error that happened.
   *
   * @param failure An error that happened when trying to process the insert.
   * @return Return the updated {@link InsertAttempt}, must be the same instance the method was
   *     called on.
   */
  InsertAttempt maybeAddFailure(Throwable failure);

  /**
   * Compares the position of this attempt to another.
   *
   * @param other the object to be compared.
   * @return Result of {@link Integer#compare(int, int)}
   */
  @Override
  default int compareTo(InsertAttempt other) {
    return Integer.compare(position(), other.position());
  }
}
