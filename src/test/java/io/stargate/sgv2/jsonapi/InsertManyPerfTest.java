package io.stargate.sgv2.jsonapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InsertManyPerfTest {

  // Base URL for the API
  private static final String API_URL = System.getenv("API_URL");

  private static final String API_TOKEN = System.getenv("API_TOKEN");

  // Number of concurrent users
  private static final int NUM_USERS = 50;

  // Duration to run the simulation (in seconds)
  private static final long RUN_DURATION_SECONDS = 10 * 60;

  // Maximum number of requests per user in the given duration (in milliseconds)
  private static final long RUN_DURATION_MS = TimeUnit.SECONDS.toMillis(RUN_DURATION_SECONDS);

  private static final ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper

  public static void main(String[] args) {
    ExecutorService executorService =
        Executors.newFixedThreadPool(NUM_USERS); // Limit to 200 threads

    // Get the end time (current time + 5 minutes)
    long endTime = System.currentTimeMillis() + RUN_DURATION_MS;

    // Submit 200 tasks to simulate 200 users
    for (int i = 1; i <= NUM_USERS; i++) {
      final int userId = i;
      executorService.submit(
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              try {
                sendRequestsSequentially(userId, endTime);
              } catch (Exception e) {
                  e.printStackTrace();
                System.out.println("Error for user " + userId + ": " + e.getMessage());
              }
              return null;
            }
          });
    }

    // Wait for all tasks to complete before shutting down executor service
    try {
      executorService.shutdown();
      executorService.awaitTermination(
          RUN_DURATION_MS + 10000, TimeUnit.MILLISECONDS); // Wait slightly longer than 5 minutes
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    System.out.println("Simulation completed.");
  }

  private static void sendRequestsSequentially(int userId, long endTime) throws Exception {
    // Send requests sequentially for each user as long as the 5-minute window is active
    while (System.currentTimeMillis() < endTime) {
      sendRequest(userId);
    }
  }

  private static void sendRequest(int userId) throws Exception {
    URL url = new URL(API_URL);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Accept", "application/json");
    connection.setRequestProperty("Token", API_TOKEN);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    // Construct the dynamic JSON payload
    String jsonPayload = buildJsonPayload(userId);

    // Write the payload to the request body
    try (OutputStream os = connection.getOutputStream()) {
      byte[] input = jsonPayload.getBytes("utf-8");
      os.write(input, 0, input.length);
    }

    // Get the response code
    int responseCode = connection.getResponseCode();
    // System.out.println("Response Code for user " + userId + ": " + responseCode);

    // If response code is 200, read and print the response body (JSON)
    if (responseCode == HttpURLConnection.HTTP_OK) {
      try (BufferedReader in =
          new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
        String inputLine;
        StringBuilder response = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
          response.append(inputLine);
        }

        // Parse the response JSON using Jackson's ObjectMapper
        JsonNode responseJson = objectMapper.readTree(response.toString());

        // Print the response JSON
        // System.out.println("Response JSON for user " + userId + ": " + responseJson.toString());

        // Check for the "errors" field in the response
        if (responseJson.has("errors")) {
          System.out.println("ERROR: 'errors' field found in response for user " + userId);
          System.out.println("Errors: " + responseJson.get("errors").toString());
        }
      }
    } else {
      // If not 200, print error message
      System.out.println("Error: Received non-200 response code for user " + userId);
    }
  }

  private static String buildJsonPayload(int userId) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < 50; i++) {
      // Build the JSON with dynamic name and random vectorize field
      String name = "test_" + userId;
      String vectorize = generateRandomTokens(100);
      sb.append(String.format("{\"name\": \"%s\", \"$vectorize\": \"%s\"}", name, vectorize));
      if (i < 49) {
        sb.append(",");
      }
    }
    return String.format("{\"insertMany\": {\"documents\": [%s]}}", sb);
  }

  private static String generateRandomTokens(int numTokens) {
    // Use a random number generator for token generation
    Random random = new Random();
    StringBuilder tokens = new StringBuilder();
    for (int i = 0; i < numTokens; i++) {
      tokens.append(generateRandomWord(random));
      if (i < numTokens - 1) {
        tokens.append(" "); // Space between tokens
      }
    }
    return tokens.toString();
  }

  private static String generateRandomWord(Random random) {
    // Generate a random word (e.g., 3-10 characters long)
    int length = random.nextInt(8) + 3;
    StringBuilder word = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      char c = (char) (random.nextInt(26) + 'a'); // Random lowercase letters
      word.append(c);
    }
    return word.toString();
  }
}
