/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.ml;

import org.json.JSONObject;
import com.google.protobuf.util.JsonFormat;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.beam.sdk.values.PCollectionTuple;
import com.google.api.client.json.GenericJson;
import com.google.cloud.recommendationengine.v1beta1.UserEvent;
import com.google.cloud.recommendationengine.v1beta1.EventStoreName;
import com.google.cloud.recommendationengine.v1beta1.InputConfig;
import com.google.cloud.recommendationengine.v1beta1.UserEventServiceClient;
import com.google.cloud.recommendationengine.v1beta1.ImportUserEventsRequest;
import com.google.cloud.recommendationengine.v1beta1.UserEventInlineSource;
import org.apache.beam.sdk.values.TupleTag;

// TODO: update documentation
/**
 * A {@link PTransform} connecting to the Recommendations AI API (https://cloud.google.com/recommendations) and
 * creating {@link UserEvent}s.
 * *
 * <p>Batch size defines how many items are at once per batch (default: 5000).
 *
 * <p>The transform consumes {@link KV} of  {@link String} and {@link GenericJson}s (assumed to be the user event id as key and
 * contents as value) and outputs a PCollectionTuple which will contain the successfully created and failed user events.
 *
 * <p>It is possible to provide a catalog name to which you want to add the catalog
 * item (defaults to "default_catalog").
 * It is possible to provide a event store to which you want to add the user
 * event (defaults to "default_event_store"). 
 */

@AutoValue
public abstract class RecommendationAIImportUserEvents
    extends PTransform<
        PCollection<KV<String, String>>, PCollection<KV<String, PCollectionTuple>>> {


    /** @return ID of Google Cloud project to be used for creating user events. */
    public abstract String projectId();

    /** @return Name of the catalog where the user events will be created. */
    public abstract @Nullable String catalogName();
    
    /** @return Name of the event store where the user events will be created. */
    public abstract @Nullable String eventStore();

    public abstract TupleTag<UserEvent> successTag();

    public abstract TupleTag<UserEvent> failureTag();
    
    /** @return Size of input elements batch to be sent to Cloud DLP service in one request. */
    public abstract Integer batchSize();

  @AutoValue.Builder
  public abstract static class Builder {
    /** @param projectId ID of Google Cloud project to be used for creating user events. */
    public abstract Builder setProjectId(String projectId);

    /** @param catalogName Name of the catalog where the user events will be created. */
    public abstract Builder setCatalogName(@Nullable String catalogName);

    /** @param eventStore Name of the event store where the user events will be created. */
    public abstract Builder setEventStore(@Nullable String eventStore);

    public abstract Builder setSuccessTag(TupleTag<UserEvent> successTag);

    public abstract Builder setFailureTag(TupleTag<UserEvent> failureTag);
   
    /**
     * @param batchSize Amount of input elements to be sent to Recommendation AI service in one request.
     */
    public abstract Builder setBatchSize(Integer batchSize);
    
    public abstract RecommendationAICreateUserEvents build();    
  }

  public static Builder newBuilder() {
    return new AutoValue_RecommendationAIImportUserEventss.Builder().setCatalogName("default_catalog").setEventStore("default_event_store");    
  }

  /**
   * The transform converts the contents of input PCollection into {@link UserEvents}s and then calls
   * the Recommendation AI service to create the user event.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollectionTuple expand(
      PCollection<KV<String, GenericJson>> input) {
    return input
        .apply("Batch Contents", ParDo.of(new BatchRequestForRecommendationAI(batchSize())))
        .apply("Import CatalogItems", ParDo.of(new ImportUserEvents(catalogName(), eventStore(), successTag(), failureTag())).withOutputTags(successTag(), TupleTagList.of(failureTag())));
  }

  static class ImportUserEvents
      extends DoFn<KV<String, GenericJson>, KV<String, UserEvent>> {
        private final String projectId;
        private final String catalogName;
        private final String eventStore;
        private final TupleTag<UserEvent> successTag;
        private final TupleTag<UserEvent> failureTag;

    /**
     * @param projectId ID of GCP project to be used for creating user events.
     * @param catalogName Catalog name for UserEvent creation.
     * @param eventStore Event store name for UserEvent creation.
     * @param successTag TupleTag for successfully created items.
     * @param failureTag TupleTag for failed items.
     */
    public ImportUserEvents(String projectId, String catalogName, String eventStore, TupleTag<UserEvent> successTag,
      TupleTag<UserEvent> failureTag) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.eventStore = eventStore;
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      EventStoreName parent = EventStoreName.of(projectId, "global", catalogName, eventStore);

      ArrayList<UserEvent> userEvents = new ArrayList<>();
      for(GenericJson element : c.element().getValue().iterateAll()) {
        UserEvent.Builder userEventBuilder = UserEvent.newBuilder();
        userEvents.add(JsonFormat.parser().merge((new JSONObject(element)).toString(), userEventBuilder));
      }
      UserEventInlineSource userEventInlineSource = new UserEventInlineSource.newBuilder().addAllUserEvents(userEvents).build();

      InputConfig inputConfig = InputConfig.newBuilder().mergeUserEventInlineSource(userEventInlineSource).build();
      ImportUserEventsRequest request = ImportUserEventsRequest.newBuilder()
        .setParent(parent.toString())
        .setInputConfig(inputConfig)
        .build();
        try (UserEventServiceClient userEventServiceClient = UserEventServiceClient.create()) {
          ImportUserEventsResponse response = userEventServiceClient.importUserEventsAsync(request).get();
      } catch (Exception e) {
        // TODO: what to do with output
      }
    }
  }
}
