package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.core.instrument.Tag;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import jakarta.ws.rs.core.Response;
import java.util.*;
import java.util.function.Predicate;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Public API model for an error or warning returned from the API.
 *
 * <p>Use {@link CommandErrorFactory} to get an instance because it know how to adapt from any
 * throwable using the {@link CommandErrorV2.Builder}.
 *
 * <p>See {@link io.stargate.sgv2.jsonapi.exception.APIException} for discussion of the fields.
 */
public record CommandErrorV2(
    @JsonProperty(ErrorObjectV2Constants.Fields.ID) UUID id,
    @JsonProperty(ErrorObjectV2Constants.Fields.FAMILY) String family,
    @JsonProperty(ErrorObjectV2Constants.Fields.SCOPE) String scope,
    @JsonProperty(ErrorObjectV2Constants.Fields.CODE) String errorCode,
    @JsonProperty(ErrorObjectV2Constants.Fields.TITLE) String title,
    @JsonProperty(ErrorObjectV2Constants.Fields.MESSAGE) String message,
    @JsonIgnore @Schema(hidden = true) Response.Status httpStatus,
    @JsonProperty(ErrorObjectV2Constants.Fields.EXCEPTION_CLASS)
        @Schema(hidden = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String exceptionClass,
    @JsonIgnore @Schema(hidden = true) List<Tag> metricTags,

    // Optional, nullable, list of the documents related to this error
    @JsonProperty(ErrorObjectV2Constants.Fields.DOCUMENT_IDS)
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<DocRowIdentifer> documentIds) {

  public CommandErrorV2 {

    Objects.requireNonNull(id, "id cannot be null");
    requireNoNullOrBlank(family, "family cannot be null or blank");
    scope = scope == null ? "" : scope;
    requireNoNullOrBlank(errorCode, "errorCode cannot be null or blank");
    requireNoNullOrBlank(title, "title cannot be null or blank");
    requireNoNullOrBlank(message, "message cannot be null or blank");
    Objects.requireNonNull(httpStatus, "httpStatus cannot be null");
    // exceptionClass is not required, it is only passed when we are in debug mode
    // normalise to null if blank
    exceptionClass = exceptionClass == null || exceptionClass.isBlank() ? null : exceptionClass;
    metricTags = metricTags == null ? Collections.emptyList() : List.copyOf(metricTags);
    documentIds = documentIds == null ? Collections.emptyList() : List.copyOf(documentIds);
  }

  private static String requireNoNullOrBlank(String value, String message) {
    if (Objects.isNull(value) || value.isBlank()) {
      throw new IllegalArgumentException(message);
    }
    return value;
  }

  /**
   * Gets a Predicate that will match this Error with other errors based on all fields except the
   * ID, used when aggregating errors with the same information in them.
   */
  public Predicate<CommandErrorV2> nonIdentityMatcher() {
    return other ->
        (family() == null || family().equals(other.family()))
            && (scope() == null || scope().equals(other.scope()))
            && (errorCode() == null || errorCode().equals(other.errorCode()))
            && (title() == null || title().equals(other.title()))
            && (message() == null || message().equals(other.message()));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private UUID id;
    private String family;
    private String scope;
    private String errorCode;
    private String title;
    private String message;
    private Response.Status httpStatus;
    private String exceptionClass;
    private List<Tag> metricsTags;
    private List<DocRowIdentifer> documentIds;

    private Builder() {}

    public Builder errorCode(String errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    public Builder message(String message) {
      this.message = message;
      return this;
    }

    public Builder httpStatus(Response.Status httpStatus) {
      this.httpStatus = httpStatus;
      return this;
    }

    public Builder exceptionClass(String errorClass) {
      this.exceptionClass = errorClass;
      return this;
    }

    public Builder metricsTags(List<Tag> metricsTags) {
      this.metricsTags = metricsTags;
      return this;
    }

    public Builder family(String family) {
      this.family = family;
      return this;
    }

    public Builder scope(String scope) {
      this.scope = scope;
      return this;
    }

    public Builder title(String title) {
      this.title = title;
      return this;
    }

    public Builder id(UUID id) {
      this.id = id;
      return this;
    }

    public Builder documentIds(List<? extends DocRowIdentifer> documentIds) {
      this.documentIds =
          documentIds == null
              ? Collections.emptyList()
              : documentIds.stream().map(d -> (DocRowIdentifer) d).toList();
      return this;
    }

    public CommandErrorV2 build() {
      return new CommandErrorV2(
          id,
          family,
          scope,
          errorCode,
          title,
          message,
          httpStatus,
          exceptionClass,
          metricsTags,
          documentIds);
    }
  }
}
