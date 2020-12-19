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
public class RecommendationAIPredictIT {
    @Rule public TestPipeline testPipeline = TestPipeline.create();

    public static GenericJson getUserEvent() {
        GenericJson userInfo = new GenericJson().set("visitorId", "1");
        return new GenericJson().set("eventType", "page-visit").set("userInfo", userInfo);
    }

    @Test
    public void predict() {
        String projectId = testPipeline.getOptions().as(GcpOptions.class).getProject();
        PCollection<KV<String, PredictResponse.PredictionResult>> predictResult =
            testPipeline
                .apply(Create.of(getUserEvent()))
                .apply(RecommendationAIPredict.newBuilder()
                    .setProjectId(projectId)
                    .setPlacementId("recently_viewed_default")
                    );
        PAssert.that(predictResult).satisfies(new VerifyPredictResult());
        testPipeline.run().waitUntilFinish();
    }

    private static class VerifyPredictResult implements SerializableFunction<Iterable<UserEvent>, Void> {
        @Override
        public Void apply(Iterable<PredictResponse.PredictionResult> input) {
            List<PredictResponse.PredictionResult> matches = new ArrayList<>();
            input.forEach(
                item -> {
                    // List<Finding> resultList = item.getValue().getResult().getFindingsList();
                    // matches.add(
                    //     resultList.stream()
                    //         .anyMatch(finding -> finding.getInfoType().equals(emailAddress)));
                }
            );
            // TODO: compare UserEvent
            assertTrue();
            return null;
        }
    }
}