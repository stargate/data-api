package io.stargate.sgv2.jsonapi.exception.playing;

import java.util.UUID;

/**
 * The information to use when creating an error, included as a record to make it easier to pass
 * around between the template and the subclasses of the {@link APIException}.
 *
 * <p>Created by the {@link ErrorTemplate} and accepeted by the {@link APIException}.
 *
 * <p>Not intended to replace the {@link APIException} or be anything that is used outside the
 * exception package.
 *
 * <p>Will make it easier if / when we add more info to an error such as links to documentation.
 * They can be passed from the template through the ErrorInstance into to the APIException.
 *
 * <p>
 *
 * @param errorId
 * @param family
 * @param scope
 * @param code
 * @param title
 * @param body
 */
public record ErrorInstance(
    UUID errorId, ErrorFamily family, ErrorScope scope, String code, String title, String body) {}
