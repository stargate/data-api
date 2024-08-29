package io.stargate.sgv2.jsonapi.exception.playing;

/**
 * Errors related to the filter clause in a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class FilterException extends RequestException {
  public FilterException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<FilterException> {
    MULTIPLE_ID_FILTER,
    FIELDS_LIMIT_VIOLATION;

    private final ErrorTemplate<FilterException> template;

    Code() {
      template =
          ErrorTemplate.load(FilterException.class, ErrorFamily.SERVER, Scope.FILTER, name());
    }

    @Override
    public ErrorTemplate<FilterException> template() {
      return template;
    }
  }
}
