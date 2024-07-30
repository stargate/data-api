package io.stargate.sgv2.jsonapi.service.resolver;

/**
 * POC for an exception to be thrown when something in the operations tier is asked to an operation
 * that is invalid, and should have been caught at the API validation phase.
 *
 * <p>e.g. API Validation should check that the fields listed in a filter exist on the target table.
 *
 * <p>TODO: this is POC until we work on the API tier validations for tables (and refactor
 * collection validation)
 */
public class UnvalidatedClauseException extends RuntimeException {

  public UnvalidatedClauseException(String message) {
    super(message);
  }
}
