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

import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

public class PubSubMessageFields extends DoFn<PubsubMessage, String> {
  private List<String> headers;

  public PubSubMessageFields(List<String> headers) {
    this.headers = headers;
  }

  @ProcessElement
  public void processContext(ProcessContext c) {
    headers.forEach(
        header -> {
          c.output(header);
        });
  }
}
