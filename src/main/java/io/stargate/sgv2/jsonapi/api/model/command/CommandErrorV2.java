package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.micrometer.core.instrument.Tag;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.*;
import java.util.function.Predicate;

/**
 * See {@link CommandError} for why this class exists.
 *
 * <p>Use either {@link #builderV1()} or {@link #builder()} to get te builder to create an
 * instance of this class.
 *
 * <p><b>Note:</b> This class is expected to be serialised to JSON for the responses message, and we
 * are not using a Java record because 1) they do not support inheritance 2) we want to (eventually)
 * lock down the constuctor so all errors are built through the builder. So uses bean naming to keep
 * Jackson happy.
 */
public record CommandErrorV2 (
    UUID id,
    String family,
    String scope,
    String errorCode,
    String title,
    String message,
    @JsonIgnore
    @Schema(hidden = true)
    Response.Status httpStatus,
    @Schema(hidden = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String errorClass,
    @JsonIgnore
    @Schema(hidden = true)
    List<Tag> metricTags,
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    List<DocumentId> documentIds
) {

  public CommandErrorV2 {

    Objects.requireNonNull(id, "id cannot be null");
    requireNoNullOrBlank(family, "family cannot be null or blank");
    scope = scope == null ? "" : scope;
    requireNoNullOrBlank(errorCode, "errorCode cannot be null or blank");
    requireNoNullOrBlank(title, "title cannot be null or blank");
    requireNoNullOrBlank(message, "message cannot be null or blank");
    Objects.requireNonNull(httpStatus, "httpStatus cannot be null");
    // errorClass is not required, it is only passed when we are in debug mode
    // normalise to null if blank
    errorClass = errorClass == null || errorClass.isBlank() ? null : errorClass;
    metricTags = metricTags == null ? Collections.emptyList() : List.copyOf(metricTags);
    documentIds = documentIds == null ? Collections.emptyList() : List.copyOf(documentIds);
  }

  private static String requireNoNullOrBlank(String value, String message) {
    if (Objects.isNull(value) || value.isBlank()) {
      throw new IllegalArgumentException(message);
    }
    return value;
  }

  // XXX TO COMMENT
  public Predicate<CommandErrorV2> nonIdentityMatcher() {
    return other ->
        (family() == null || family().equals(other.family())) &&
            (scope() == null || scope().equals(other.scope())) &&
            (errorCode() == null || errorCode().equals(other.errorCode())) &&
            (title() == null || title().equals(other.title())) &&
            (message() == null || message().equals(other.message()))
        ;
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
    private String errorClass;
    private List<Tag> metricsTags;
    private List<DocumentId> documentIds;

    private Builder() {
    }

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

    public Builder errorClass(String errorClass) {
      this.errorClass = errorClass;
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

    public Builder documentIds(List<DocumentId> documentIds) {
      this.documentIds = documentIds;
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
          errorClass,
          metricsTags,
          documentIds);
    }
  }
}
