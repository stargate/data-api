package io.stargate.sgv2.jsonapi.api.model.command;

import jakarta.ws.rs.core.Response;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * See {@link CommandError} for why this class exists.
 *
 * <p>Use either {@link #builderV1()} or {@link #builderV2()} to get te builder to create an
 * instance of this class.
 *
 * <p><b>Note:</b> This class is expected to be serialised to JSON for the responses message, and we
 * are not using a Java record because 1) they do not support inheritance 2) we want to (eventually)
 * lock down the constuctor so all errors are built through the builder. So uses bean naming to keep
 * Jackson happy.
 */
public class CommandErrorV2 extends CommandError {

  private final String family;
  private final String scope;
  private final String title;
  private final UUID id;

  public CommandErrorV2(
      String errorCode,
      String message,
      Response.Status httpStatus,
      String errorClass,
      Map<String, Object> metricsTags,
      String family,
      String scope,
      String title,
      UUID id) {
    super(errorCode, message, httpStatus, errorClass, metricsTags);
    this.family = requireNoNullOrBlank(family, "family cannot be null or blank");
    this.scope = scope == null ? "" : scope;
    this.title = requireNoNullOrBlank(title, "title cannot be null or blank");
    this.id = Objects.requireNonNull(id, "id cannot be null");
  }

  /** Create a new builder for the {@link CommandError} that represents the V1 error object. */
  public static Builder<CommandError> builderV1() {
    return new Builder<>(true);
  }

  /** Create a new builder for the {@link CommandErrorV2} that represents the V2 error object. */
  public static Builder<CommandErrorV2> builderV2() {
    return new Builder<>(false);
  }

  public String getFamily() {
    return family;
  }

  public String getScope() {
    return scope;
  }

  public String getTitle() {
    return title;
  }

  public UUID getId() {
    return id;
  }

  public static class Builder<T extends CommandError> {
    private String errorCode;
    private String message;
    private Response.Status httpStatus;
    private String errorClass;
    private Map<String, Object> metricsTags;
    private String family;
    private String scope;
    private String title;
    private UUID id;

    private final boolean v1Error;

    Builder(boolean v1Error) {
      this.v1Error = v1Error;
    }

    public Builder<T> errorCode(String errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    public Builder<T> message(String message) {
      this.message = message;
      return this;
    }

    public Builder<T> httpStatus(Response.Status httpStatus) {
      this.httpStatus = httpStatus;
      return this;
    }

    public Builder<T> errorClass(String errorClass) {
      this.errorClass = errorClass;
      return this;
    }

    public Builder<T> metricsTags(Map<String, Object> metricsTags) {
      this.metricsTags = metricsTags;
      return this;
    }

    public Builder<T> family(String family) {
      this.family = family;
      return this;
    }

    public Builder<T> scope(String scope) {
      this.scope = scope;
      return this;
    }

    public Builder<T> title(String title) {
      this.title = title;
      return this;
    }

    public Builder<T> id(UUID id) {
      this.id = id;
      return this;
    }

    public T build() {
      return v1Error
          ? unchecked(new CommandError(errorCode, message, httpStatus, errorClass, metricsTags))
          : unchecked(
              new CommandErrorV2(
                  errorCode,
                  message,
                  httpStatus,
                  errorClass,
                  metricsTags,
                  family,
                  scope,
                  title,
                  id));
    }

    @SuppressWarnings("unchecked")
    private T unchecked(CommandError error) {
      return (T) error;
    }
  }
}
