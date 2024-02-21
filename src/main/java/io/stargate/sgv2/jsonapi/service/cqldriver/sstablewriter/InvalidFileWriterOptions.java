package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

public class InvalidFileWriterOptions extends Exception {
  public InvalidFileWriterOptions(String message) {
    super(message);
  }
}
