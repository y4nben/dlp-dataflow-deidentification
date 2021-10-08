/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.swarm.tokenization.pubsub;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class ConvertPubSubMessageToDLPRow extends DoFn<PubsubMessage, KV<String, Table.Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(ConvertPubSubMessageToDLPRow.class);
  public Gson gson;

  @Setup
  public void setup() {
    gson = new Gson();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    PubsubMessage message = c.element();
    String messageId = message.getMessageId();
    Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
    JsonObject json = gson.fromJson(new String(message.getPayload(), UTF_8), JsonObject.class);

    json.keySet()
        .forEach(
            key -> {
              String value = json.get(key).getAsString();
              tableRowBuilder.addValues(Value.newBuilder().setStringValue(value));
            });
    Table.Row dlpRow = tableRowBuilder.build();
    LOG.debug("MessageId: {}", messageId);
    LOG.debug("DLP Row {}", dlpRow);
    c.output(KV.of(messageId, dlpRow));
  }
}
