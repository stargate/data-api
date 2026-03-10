package io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class  DynamicTestExecutable implements Executable {

  private final String description;
  private final TestUri testUri;
  private final Executable executable;

  private final String trimmedDisplayName;
  private final boolean isTrimmed;

  public  DynamicTestExecutable(String description, TestUri.Builder testUri, Executable executable) {
    this(description, testUri.build(), executable);
  }

  @SuppressWarnings("StringEquality")
  public  DynamicTestExecutable(String description, TestUri testUri, Executable executable) {
    this.description = description;
    this.testUri = testUri;
    this.executable = executable;

    var truncated =  ( description != null && description.length() > 60) ?
        description.substring(0, 57) + "..."
        :
        description;

    this.trimmedDisplayName = truncated;
    this.isTrimmed = truncated != description;
  }

  public String trimmedDisplayName(){
    return  trimmedDisplayName;
  }

  public DynamicTest testNode(){
    return dynamicTest(trimmedDisplayName, testUri.uri(), this);
  }

  @Override
  public void execute() throws Throwable {
    beforeExecute();
    executable.execute();
    afterExecute();
  }

  private void beforeExecute(){
    if (isTrimmed) {
      System.out.printf(description + "\n");
    }
  }

  private void afterExecute(){
    System.out.printf("Executed - " + testUri.uri().toString() + "\n");
  }
}
