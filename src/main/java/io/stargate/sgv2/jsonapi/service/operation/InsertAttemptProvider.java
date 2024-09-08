package io.stargate.sgv2.jsonapi.service.operation;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.function.Function;

/**
 * Functional interface for a provider that can create an {@link InsertAttempt} from a {@link RawShreddedDocument}.
 * @param <T>
 */
@FunctionalInterface
public interface InsertAttemptProvider<T extends InsertAttempt> extends Function<JsonNode, T> {
}
