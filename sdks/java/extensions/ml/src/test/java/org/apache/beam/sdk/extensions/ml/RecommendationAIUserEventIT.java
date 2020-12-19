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
public class RecommendationAIUserEventIT {
    @Rule public TestPipeline testPipeline = TestPipeline.create();

    public static GenericJson getUserEvent() {
        GenericJson userInfo = new GenericJson().set("visitorId", "1");
        return new GenericJson().set("eventType", "page-visit").set("userInfo", userInfo);
    }

    @Test
    public void createUserEvent() {
        String projectId = testPipeline.getOptions().as(GcpOptions.class).getProject();
        PCollection<KV<String, UserEvent>> createUserEventResult =
            testPipeline
                .apply(Create.of(getUserEvent()))
                .apply(RecommendationAIWriteUserEvent.newBuilder()
                    .setProjectId(projectId)
                    );
        PAssert.that(createUserEventResult).satisfies(new VerifyCreateUserEventResult());
        testPipeline.run().waitUntilFinish();
    }

    @Test void importUserEvents() {
        String projectId = testPipeline.getOptions().as(GcpOptions.class).getProject();
        Iterable<KV<String, GenericJson>> userEvents = new ArrayList<>();
        userEvents.add(KV.of("123", getUserEvent()));
        userEvents.add(KV.of("123", getUserEvent()));
        PCollection<CatalogItem> importUserEventResult =
            testPipeline
                .apply(Create.of(userEvents))
                .apply(RecommendationAIImportUserEvents.newBuilder()
                    .setProjectId(projectId)
                    );
        PAssert.that(importUserEventResult).satisfies(new VerifyImportUserEventsResult());
        testPipeline.run().waitUntilFinish();

    }

    private static class VerifyCreateUserEventResult implements SerializableFunction<Iterable<UserEvent>, Void> {
        @Override
        public Void apply(Iterable<UserEvent> input) {
            List<UserEvent> matches = new ArrayList<>();
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
            // TODO: count number of UserEvents
            assertTrue();
            return null;
        }
    }

    private static class VerifyImportUserEventsResult implements SerializableFunction<Iterable<UserEvent>, Void> {
        @Override
        public Void apply(Iterable<UserEvent> input) {            
            List<UserEvent> matches = new ArrayList<>();
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