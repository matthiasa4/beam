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

import java.util.Random;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import com.google.cloud.recommendationengine.v1beta1.CatalogItem;
import org.apache.beam.sdk.values.KV;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class RecommendationAICatalogItemIT {
    @Rule public TestPipeline testPipeline = TestPipeline.create();

    private static GenericJson getCatalogItem() {
        List<Object> categories = new ArrayList<Object>();
        categories.add(new GenericJson().set("categories", Arrays.asList("Electronics", "Computers")));
        categories.add(new GenericJson().set("categories", Arrays.asList("Laptops")));
        return new GenericJson().set("id", Integer.toString(new Random().nextInt())).set("title", "Sample Laptop")
                .set("description", "Indisputably the most fantastic laptop ever created.")
                .set("categoryHierarchies", categories).set("languageCode", "en");
    }

    @Test
    public void createCatalogItem() {
        String projectId = testPipeline.getOptions().as(GcpOptions.class).getProject();
        PCollection<KV<String, CatalogItem>> createCatalogItemResult =
            testPipeline
                .apply(Create.of(getCatalogItem()))
                .apply(RecommendationAICreateCatalogItem.newBuilder()
                    .setProjectId(projectId)
                    );
        PAssert.that(createCatalogItemResult).satisfies(new VerifyCreateCatalogItemResult());
        testPipeline.run().waitUntilFinish();
    }

    @Ignore("Import method causing issues")
    @Test void importCatalogItems() {
        String projectId = testPipeline.getOptions().as(GcpOptions.class).getProject();
        Iterable<KV<String, GenericJson>> catalogItems = new ArrayList<>();
        catalogItems.add(KV.of(Integer.toString(new Random().nextInt()), getCatalogItem()));
        catalogItems.add(KV.of(Integer.toString(new Random().nextInt()), getCatalogItem()));
        PCollection<CatalogItem> importCatalogItemResult =
            testPipeline
                .apply(Create.of(catalogItems))
                .apply(RecommendationAIImportCatalogItems.newBuilder()
                    .setProjectId(projectId)
                    );
        PAssert.that(importCatalogItemResult).satisfies(new VerifyImportCatalogItemsResult());
        testPipeline.run().waitUntilFinish();
    }

    private static class VerifyCreateCatalogItemResult implements SerializableFunction<Iterable<CatalogItem>, Void> {
        @Override
        public Void apply(Iterable<CatalogItem> input) {
            List<CatalogItem> matches = new ArrayList<>();
            input.forEach(
                item -> {
                    CatalogItem result = item.getId();
                    matches.add(result);
                }
            );
            assertTrue(matches.contains((String) getCatalogItem().getValueMap().get("id")));
            assertEquals(1, matches.size());
            return null;
        }
    }

    private static class VerifyImportCatalogItemsResult implements SerializableFunction<Iterable<CatalogItem>, Void> {
        @Override
        public Void apply(Iterable<CatalogItem> input) {            
            List<CatalogItem> matches = new ArrayList<>();
            input.forEach(
                item -> {
                    // List<Finding> resultList = item.getValue().getResult().getFindingsList();
                    // matches.add(
                    //     resultList.stream()
                    //         .anyMatch(finding -> finding.getInfoType().equals(emailAddress)));
                }
            );
            // TODO: compare CatalogItem
            assertTrue();
            // TODO: count number of CatalogItems
            assertTrue();
            return null;
        }
    }
}