package io.stargate.sgv2.jsonapi.exception;

/**
 * Error Related to Cassandra Authentication.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class CassandraAuthenticationException extends APIException {

    public static final ErrorFamily FAMILY = ErrorFamily.SERVER;

    public CassandraAuthenticationException(ErrorInstance message) {
        super(message);
    }

    /**
     * Specialization of the error for Authentication Errors
     */
    public enum Code implements ErrorCode<CassandraAuthenticationException> {
        TOO_MANY_ATTEMPT_UNSUCCESSFUL_ATTEMPT,
        AUTHENTICATION_FAILED;

        private final ErrorTemplate<CassandraAuthenticationException> template;

        Code() {
            template = ErrorTemplate.load(CassandraAuthenticationException.class, FAMILY, ErrorScope.NONE, name());
        }

        @Override
        public ErrorTemplate<CassandraAuthenticationException> template() {
            return template;
        }
    }

}
