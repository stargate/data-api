package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;

import java.util.List;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static org.hamcrest.Matchers.hasSize;


/**
 * Assertions that check the structure of the API response, e.g. should it have a `data` field
 * <p>
 * See {@link TestAssertion}
 * </p>
 */
public class Response {

  static {
    AssertionMatcher.FACTORY_REGISTRY.register(Response.class);
  }

  /**
   * Checks the hTTP status AND the shape of the response doc
   */
  public static AssertionMatcher isSuccess(TestCommand testCommand, JsonNode args) {

    var commandName = testCommand.commandName();
    return switch (commandName.getCommandType()){
      case DDL -> isDDLSuccess(testCommand, args);
      case DML   ->
         switch (commandName) {
           case FIND_ONE, FIND -> Response.isFindSuccess(testCommand, args);
           case FIND_ONE_AND_DELETE, FIND_ONE_AND_REPLACE, FIND_ONE_AND_UPDATE -> Response.isFindAndSuccess(testCommand, args);
           case INSERT_ONE, INSERT_MANY -> Response.isWriteSuccess(testCommand, args);
           default -> throw new IllegalStateException("No isSuccess mapping for command name: " + commandName);
         };
      case ADMIN -> throw new IllegalStateException("No isSuccess mapping for command name: " + commandName);
    };
  }

  public static AssertionMatcher isFindSuccess(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("$", responseIsFindSuccess());
  }

  public static AssertionMatcher isFindAndSuccess(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("$", responseIsFindAndSuccess());
  }


  public static AssertionMatcher isWriteSuccess(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("$", responseIsWriteSuccess());
  }

  public static AssertionMatcher isDDLSuccess(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("$", responseIsDDLSuccess());
  }

}
