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
package com.google.swarm.tokenization;

import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.common.BigQueryDynamicWriteTransform;
import com.google.swarm.tokenization.common.DLPTransform;
import com.google.swarm.tokenization.common.Util;
import com.google.swarm.tokenization.pubsub.ConvertPubSubMessageToDLPRow;
import com.google.swarm.tokenization.pubsub.PubSubMessageFields;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPPubSubToBigQueryStreamingV2 {

  public static final Logger LOG = LoggerFactory.getLogger(DLPPubSubToBigQueryStreamingV2.class);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(3);

  public static void main(String[] args) {

    DLPPubSubToBigQueryStreamingV2PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .as(DLPPubSubToBigQueryStreamingV2PipelineOptions.class);

    run(options);
  }

  public static PipelineResult run(DLPPubSubToBigQueryStreamingV2PipelineOptions options) {
    Pipeline p = Pipeline.create(options);
    PCollection<PubsubMessage> messages = null;
    PCollection<KV<String, Table.Row>> records;
    messages =
        p.apply(
            "ReadPubSubSubscription",
            PubsubIO.readMessagesWithAttributesAndMessageId()
                .fromSubscription(options.getInputSubscription()));
    records = messages.apply("ConvertToDLPRow", ParDo.of(new ConvertPubSubMessageToDLPRow()));
    LOG.debug("Test log");
    final PCollectionView<List<String>> header =
        messages
            .apply(
                "GlobalWindow",
                Window.<PubsubMessage>into(new GlobalWindows())
                    .triggering(
                        Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes())
            .apply("Readheader", ParDo.of(new PubSubMessageFields(options.getHeaders())))
            .apply("ViewAsList", View.asList());
    LOG.debug(header.toString());
    records
        .apply("Fixed Window", Window.into(FixedWindows.of(WINDOW_INTERVAL)))
        .apply(
            "DLPTransform",
            DLPTransform.newBuilder()
                .setBatchSize(options.getBatchSize())
                .setInspectTemplateName(options.getInspectTemplateName())
                .setDeidTemplateName(options.getDeidentifyTemplateName())
                .setDlpmethod(options.getDLPMethod())
                .setProjectId(options.getDLPParent())
                .setHeader(header)
                .setColumnDelimiter(options.getColumnDelimiter())
                .setJobName(options.getJobName())
                .build())
        .get(Util.inspectOrDeidSuccess)
        .apply(
            "StreamInsertToBQ",
            BigQueryDynamicWriteTransform.newBuilder()
                .setDatasetId(options.getDataset())
                .setProjectId(options.getProject())
                .build());

    return p.run();
  }
}

/*
  gradle run -DmainClass=com.google.swarm.tokenization.DLPPubSubToBigQueryStreamingV2 -Pargs=" --region=us-west1 --project=benlearnsdataflow --streaming --enableStreamingEngine --tempLocation=gs://dataflowtestbucket123/temp --numWorkers=1 --maxNumWorkers=2 --runner=DataflowRunner --dataset=dataflowtest   --inspectTemplateName=projects/benlearnsdataflow/inspectTemplates/template-test --deidentifyTemplateName=projects/benlearnsdataflow/deidentifyTemplates/2634454279133006769 --inputSubscription=projects/benlearnsdataflow/subscriptions/deid-testing-sub --headers=ssn,email,age"
  gradle run -DmainClass=com.google.swarm.tokenization.DLPPubSubToBigQueryStreamingV2 -Pargs=" --region=us-west1 --project=benlearnsdataflow --streaming --enableStreamingEngine --tempLocation=gs://dataflowtestbucket123/temp --numWorkers=1 --maxNumWorkers=2 --runner=DataflowRunner --dataset=dataflowtest   --inspectTemplateName=./resources/inspect-template.json --deidentifyTemplateName=./resources/deid-template-no-key.jsonß --inputSubscription=deid-testing"


 */
