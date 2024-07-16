package io.stargate.sgv2.jsonapi.exception.playing;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

// TODO: Docs base for all exceptions
public abstract class APIException extends RuntimeException {


  public final UUID uuid = UUID.randomUUID();
  public final ErrorFamily family;
  public final String statusCode;
  public final String title;
  public final String message;

  public APIException(ErrorFamily family, String statusCode, String title, String message) {
    this.family = family;
    this.statusCode = statusCode;
    this.title = title;
    this.message = message;
  }

  public CommandResponseError getCommandResponseError() {
    // Where we build the error to return
    return null;
  }
}
