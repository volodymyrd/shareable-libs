package com.volmyr.message_bus.producer.http;

import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.producer.HttpResponse;
import com.volmyr.message_bus.producer.MessageProducer;
import com.volmyr.message_bus.producer.MessageProducerException;
import com.volmyr.message_bus.producer.MessageProducerResponse;
import com.volmyr.message_bus.producer.ResponseType;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Implementation of {@link MessageProducer} via HTTP POST request.
 */
public final class HttpPostMessageProducer implements MessageProducer {

  private final HttpURLConnection httpURLConnection;

  public HttpPostMessageProducer(String endpointUrl) throws IOException {
    httpURLConnection = (HttpURLConnection) new URL(endpointUrl).openConnection();
    httpURLConnection.setRequestMethod("POST");
  }

  @Override
  public MessageProducerResponse send(MessageEvent event) throws MessageProducerException {
    post(event);
    return getResponse(getResponseCode());
  }

  private void post(MessageEvent event) throws MessageProducerException {
    httpURLConnection.setDoOutput(true);
    try (DataOutputStream dataOutputStream =
        new DataOutputStream(httpURLConnection.getOutputStream())) {
      dataOutputStream.write(event.toByteArray());
      dataOutputStream.flush();
    } catch (IOException e) {
      throw new MessageProducerException(e);
    }
  }

  private int getResponseCode() throws MessageProducerException {
    try {
      return httpURLConnection.getResponseCode();
    } catch (IOException e) {
      throw new MessageProducerException(e);
    }
  }

  private MessageProducerResponse getResponse(int responseCode) throws MessageProducerException {
    try (BufferedReader in = new BufferedReader(
        new InputStreamReader(httpURLConnection.getInputStream()))) {
      StringBuilder response = new StringBuilder();
      String line;
      while ((line = in.readLine()) != null) {
        response.append(line);
      }
      return MessageProducerResponse.newBuilder()
          .setType(ResponseType.HTTP)
          .setHttpResponse(HttpResponse.newBuilder()
              .setResponseCode(responseCode)
              .setResponse(response.toString())
              .build())
          .build();
    } catch (IOException e) {
      throw new MessageProducerException(e);
    }
  }

  @Override
  public void close() {
    httpURLConnection.disconnect();
  }
}
