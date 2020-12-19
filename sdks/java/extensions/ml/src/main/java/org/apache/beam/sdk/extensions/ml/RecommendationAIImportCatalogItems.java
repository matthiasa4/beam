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

import com.google.cloud.recommendationengine.v1beta1.CatalogItem;
import com.google.cloud.recommendationengine.v1beta1.CatalogName;
import com.google.cloud.recommendationengine.v1beta1.CatalogServiceClient;
import com.google.cloud.recommendationengine.v1beta1.CatalogInlineSource;

import com.google.cloud.recommendationengine.v1beta1.ImportCatalogItemsRequest;


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
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import com.google.api.client.json.GenericJson;

// TODO: update documentation
/**
 * A {@link PTransform} connecting to the Recommendations AI API (https://cloud.google.com/recommendations) and
 * creating {@link CatalogItem}s.
 * *
 * <p>Batch size defines how many items are at once per batch (default: 5000).
 *
 * <p>The transform consumes {@link KV} of  {@link String} and {@link GenericJson}s (assumed to be the catalog item id as key and
 * contents as value) and outputs a PCollectionTuple which will contain the successfully created and failed catalog items.
 *
 * <p>It is possible to provide a catalog name to which you want to add the catalog
 * item (defaults to "default_catalog").
 */

@AutoValue
public abstract class RecommendationAIImportCatalogItems
    extends PTransform<
        PCollection<KV<String, String>>, PCollection<KV<String, PCollectionTuple>>> {


    /** @return ID of Google Cloud project to be used for creating catalog items. */
    public abstract String projectId();

    /** @return Name of the catalog where the catalog items will be created. */
    public abstract @Nullable String catalogName();

    public abstract TupleTag<CatalogItem> successTag();

    public abstract TupleTag<CatalogItem> failureTag();
    
    /** @return Size of input elements batch to be sent to Cloud DLP service in one request. */
    public abstract Integer batchSize();

  @AutoValue.Builder
  public abstract static class Builder {
    /** @param projectId ID of Google Cloud project to be used for creating catalog items. */
    public abstract Builder setProjectId(String projectId);

    /** @param catalogName Name of the catalog where the catalog items will be created. */
    public abstract Builder setCatalogName(@Nullable String catalogName);

    public abstract Builder setSuccessTag(TupleTag<CatalogItem> successTag);

    public abstract Builder setFailureTag(TupleTag<CatalogItem> failureTag);
   
    /**
     * @param batchSize Amount of input elements to be sent to Recommendation AI service in one request.
     */
    public abstract Builder setBatchSize(Integer batchSize);
    
    public abstract RecommendationAICreateCatalogItem build();    
  }

  public static Builder newBuilder() {
    return new AutoValue_RecommendationAIImportCatalogItems.Builder();    
  }

  /**
   * The transform converts the contents of input PCollection into {@link CatalogItem}s and then calls
   * the Recommendation AI service to create the catalog item.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollectionTuple expand(
      PCollection<KV<String, GenericJson>> input) {
    return input
        .apply("Batch Contents", ParDo.of(new BatchRequestForRecommendationAI(batchSize())))
        .apply("Import CatalogItems", ParDo.of(new ImportCatalogItems(catalogName(), successTag(), failureTag())).withOutputTags(successTag(), TupleTagList.of(failureTag())));
  }

  static class ImportCatalogItems
      extends DoFn<KV<String, Iterable<GenericJson>>, KV<String, CatalogItem>> {
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
    public ImportCatalogItems(String projectId, String catalogName, TupleTag<CatalogItem> successTag,
      TupleTag<CatalogItem> failureTag) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      CatalogName parent = CatalogName.of(projectId, "global", catalogName);

      ArrayList<CatalogItem> catalogItems = new ArrayList<>();
      for(GenericJson element : c.element().getValue().iterateAll()) {
        CatalogItem.Builder catalogItemBuilder = CatalogItem.newBuilder();
        catalogItems.add(JsonFormat.parser().merge((new JSONObject(element)).toString(), catalogItemBuilder));
      }
      CatalogInlineSource catalogInlineSource = CatalogInlineSource.newBuiler().addAllCatalogItems(catalogItems).build();

      InputConfig inputConfig = InputConfig.newBuilder().mergeCatalogInlineSource(catalogInlineSource).build();
      ImportCatalogItemsRequest request = ImportCatalogItemsRequest.newBuilder()
        .setParent(parent.toString())
        .setInputConfig(inputConfig)
        .build();
      try (CatalogServiceClient catalogServiceClient = CatalogServiceClient.create()) {
        ImportCatalogItemsResponse response = catalogServiceClient.importCatalogItemsAsync(request).get();
      } catch (Exception e) {
        // TODO: what to do with output
      }
    }
  }
}
