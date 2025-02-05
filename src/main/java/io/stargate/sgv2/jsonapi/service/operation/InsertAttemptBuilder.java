package io.stargate.sgv2.jsonapi.service.operation;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * IMPORTANT: THIS IS ALSO USED BY THE COLLECTIONS (JUST FOR INSERT) SO IT NEEDS TO STAY
 * UNTIL COLLECTIONS CODE IS UPDATED (INSERTS STARTED THE "ATTEMPT" PATTERN)
 *
 * Functional interface for a provider that can create an {@link InsertAttempt} from a {@link
 * JsonNode}.
 *
 * <p>Implementations need to take care of the shredding of the {@link JsonNode} through to creating
 * an {@link InsertAttempt} that is ready to be executed. NOTE: this means creating the insert
 * attempt should include validating the data, and throwing an exception if the data is invalid.
 *
 * @param <T> The type of {@link InsertAttempt} that will be created.
 */
@FunctionalInterface
public interface InsertAttemptBuilder<AttemptT extends InsertAttempt<?>> {

  /**
   * Create an {@link InsertAttempt} from the {@link JsonNode}.
   *
   * @param jsonNode The {@link JsonNode} to create the {@link InsertAttempt} from.
   * @return The {@link InsertAttempt} created from the {@link JsonNode}.
   */
  AttemptT build(JsonNode jsonNode);
}
