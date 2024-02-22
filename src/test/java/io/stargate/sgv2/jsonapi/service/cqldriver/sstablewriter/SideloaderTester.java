package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.processor.SideLoaderCommandProcessor;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SideloaderTester {
  public static void main(String[] args)
      throws JsonProcessingException, ExecutionException, InterruptedException {
    String namespace = "demo_namespace";
    String collection = "players";

    SideLoaderCommandProcessor sideLoaderCommandProcessor =
        new SideLoaderCommandProcessor(
            namespace,
            collection,
            true,
            CollectionSettings.SimilarityFunction.COSINE,
            3,
            null,
            "/var/tmp/sstables_test");

    // 1
    String writerSessionId = sideLoaderCommandProcessor.beginWriterSession();

    System.out.println("Writer session id: " + writerSessionId);

    // 2
    SSTableWriterStatus ssTableWriterStatus =
        sideLoaderCommandProcessor.insertDocuments(
            writerSessionId, List.of(new ObjectMapper().readTree("{\"id\": 1}")));

    System.out.println("SSTable writer status: " + ssTableWriterStatus);

    // 3
    SSTableWriterStatus intermediateStatus =
        sideLoaderCommandProcessor.getWriterStatus(writerSessionId);
    System.out.println("Intermediate status: " + intermediateStatus);

    // 4
    sideLoaderCommandProcessor.endWriterSession(writerSessionId);
    System.out.println("Session ended");

    // 5
    /*SSTableWriterStatus statusAfterRemoval =
        sideLoaderCommandProcessor.getWriterStatus(writerSessionId);
    System.out.println("Status after close: " + statusAfterRemoval);*/
  }
}
