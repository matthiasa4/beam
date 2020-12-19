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

import java.io.IOException;
import javax.annotation.Nullable;


import org.json.JSONObject;
import com.google.protobuf.util.JsonFormat;

import com.google.api.client.json.GenericJson;
import com.google.auto.value.AutoValue;
import com.google.cloud.recommendationengine.v1beta1.CatalogItem;
import com.google.cloud.recommendationengine.v1beta1.CatalogName;
import com.google.cloud.recommendationengine.v1beta1.CatalogServiceClient;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * A {@link PTransform} using the Recommendations AI API (https://cloud.google.com/recommendations). Takes an input
 * {@link PCollection} of {@link GenericJson}s and converts them to and creates
 * {@link CatalogItem}s.
 *
 * <p>
 * It is possible to provide a catalog name to which you want to add the catalog
 * item (defaults to "default_catalog").
 */

// Good example:
// https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/b870f38543efbe1a3808b7a8729fd010d1d2c755/v2/pubsub-to-mongodb/src/main/java/com/google/cloud/teleport/v2/templates/PubSubToMongoDB.java#L341
// https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/src/main/java/com/google/cloud/teleport/templates/common/JavascriptTextTransformer.java#L297
@AutoValue
public abstract class RecommendationAICreateCatalogItem extends PTransform<PCollection<GenericJson>, PCollectionTuple> {


    /** @return ID of Google Cloud project to be used for creating catalog items. */
    public abstract String projectId();

    /** @return Name of the catalog where the catalog items will be created. */
    public abstract @Nullable String catalogName();

    public abstract TupleTag<CatalogItem> successTag();

    public abstract TupleTag<CatalogItem> failureTag();

    // AutoValue
    // https://github.com/google/auto/blob/master/value/userguide/builders.md
    // https://github.com/google/auto/blob/master/value/userguide/index.md
    @AutoValue.Builder
    public abstract static class Builder {       
        /** @param projectId ID of Google Cloud project to be used for creating catalog items. */
        public abstract Builder setProjectId(String projectId);
        
        /** @param catalogName Name of the catalog where the catalog items will be created. */
        public abstract Builder setCatalogName(@Nullable String catalogName);

        public abstract Builder setSuccessTag(TupleTag<CatalogItem> successTag);

        public abstract Builder setFailureTag(TupleTag<CatalogItem> failureTag);

        public abstract RecommendationAICreateCatalogItem build();
    }

    public static Builder newBuilder() {
        return new AutoValue_RecommendationAICreateCatalogItem.Builder().setCatalogName("default_catalog");
    }

  /**
   * The transform converts the contents of input PCollection into {@link CatalogItem}s and then calls
   * the Recommendation AI service to create the catalog item.
   *
   * @param input input PCollection
   * @return PCollectionTuple with successful and failed {@link CatalogItem}s
   */

    // TODO: good example:
    // https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/b870f38543efbe1a3808b7a8729fd010d1d2c755/src/main/java/com/google/cloud/teleport/templates/common/JavascriptTextTransformer.java#L297    
    @Override
    public PCollectionTuple expand(PCollection<GenericJson> input) {
        return input.apply(ParDo.of(new CreateCatalogItem(catalogName(), successTag(), failureTag()))
                .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }

    private static class CreateCatalogItem extends DoFn<GenericJson, CatalogItem> {
        private final String projectId;
        private final String catalogName;
        private final TupleTag<CatalogItem> successTag;
        private final TupleTag<CatalogItem> failureTag;

        /**
         * @param projectId ID of GCP project to be used for creating catalog items.
         * @param catalogName Catalog name for CatalogItem creation.
         * @param successTag TupleTag for successfully created items.
         * @param failureTag TupleTag for failed items.
         */
        private CreateCatalogItem(String projectId, String catalogName, TupleTag<CatalogItem> successTag,
                TupleTag<CatalogItem> failureTag) {
            this.projectId = projectId;
            this.catalogName = catalogName;
            this.successTag = successTag;
            this.failureTag = failureTag;
        }

        @ProcessElement
        public void ProcessElement(ProcessContext context) throws IOException {
            CatalogName parent = CatalogName.of(projectId, "global", catalogName);
            CatalogItem.Builder catalogItemBuilder = CatalogItem.newBuilder();
            // https://stackoverflow.com/questions/38406211/how-to-convert-from-json-to-protobuf/38441368#38441368
            CatalogItem catalogItem = JsonFormat.parser().merge((new JSONObject(context.element)).toString(), catalogItemBuilder);

            try (CatalogServiceClient catalogServiceClient = CatalogServiceClient.create()) {
                CatalogItem response = catalogServiceClient.createCatalogItem(parent, catalogItem);

                context.output(successTag, response);
            } catch (Exception e) {
                context.output(failureTag, catalogItem);
            }
        }

    }

}