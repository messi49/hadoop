/**
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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FakeSchedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

/**
 * comparator.compare(sched1, sched2) < 0 means that sched1 should get a
 * container before sched2
 */
public class TestDominantResourceFairnessPolicy {

  private Comparator<Schedulable> createComparator(int clusterMem,
      int clusterCpu, int clusterGpuMem) {
    DominantResourceFairnessPolicy policy = new DominantResourceFairnessPolicy();
    policy.initialize(BuilderUtils.newResource(clusterMem, clusterCpu, clusterGpuMem));
    return policy.getComparator();
  }
  
  private Schedulable createSchedulable(int memUsage, int cpuUsage, int gpuMemUsage) {
    return createSchedulable(memUsage, cpuUsage, gpuMemUsage, ResourceWeights.NEUTRAL, 0, 0);
  }
  
  private Schedulable createSchedulable(int memUsage, int cpuUsage, int gpuMemUsage,
      int minMemShare, int minCpuShare) {
    return createSchedulable(memUsage, cpuUsage, gpuMemUsage, ResourceWeights.NEUTRAL,
        minMemShare, minCpuShare);
  }
  
  private Schedulable createSchedulable(int memUsage, int cpuUsage, int gpuMemUsage,
      ResourceWeights weights) {
    return createSchedulable(memUsage, cpuUsage, gpuMemUsage, weights, 0, 0);
  }

  
  private Schedulable createSchedulable(int memUsage, int cpuUsage, int gpuMemUsage,
      ResourceWeights weights, int minMemShare, int minCpuShare) {
    Resource usage = BuilderUtils.newResource(memUsage, cpuUsage, gpuMemUsage);
    Resource minShare = BuilderUtils.newResource(minMemShare, minCpuShare, gpuMemUsage);
    return new FakeSchedulable(minShare,
        Resources.createResource(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
        weights, Resources.none(), usage, 0l);
  }
  
  @Test
  public void testSameDominantResource() {
    assertTrue(createComparator(8000, 4, 256).compare(
        createSchedulable(1000, 1, 256),
        createSchedulable(2000, 1, 256)) < 0);
  }
  
  @Test
  public void testDifferentDominantResource() {
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(4000, 3, 256),
        createSchedulable(2000, 5, 256)) < 0);
  }
  
  @Test
  public void testOneIsNeedy() {
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(2000, 5, 256, 0, 6),
        createSchedulable(4000, 3, 256, 0, 0)) < 0);
  }
  
  @Test
  public void testBothAreNeedy() {
    assertTrue(createComparator(8000, 100, 256).compare(
        // dominant share is 2000/8000
        createSchedulable(2000, 5, 256),
        // dominant share is 4000/8000
        createSchedulable(4000, 3, 256)) < 0);
    assertTrue(createComparator(8000, 100, 256).compare(
        // dominant min share is 2/3
        createSchedulable(2000, 5, 256, 3000, 6),
        // dominant min share is 4/5
        createSchedulable(4000, 3, 256, 5000, 4)) < 0);
  }
  
  @Test
  public void testEvenWeightsSameDominantResource() {
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(3000, 1, 256, new ResourceWeights(2.0f)),
        createSchedulable(2000, 1, 256)) < 0);
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(1000, 3, 256, new ResourceWeights(2.0f)),
        createSchedulable(1000, 2, 256)) < 0);
  }
  
  @Test
  public void testEvenWeightsDifferentDominantResource() {
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(1000, 3, 256, new ResourceWeights(2.0f)),
        createSchedulable(2000, 1, 256)) < 0);
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(3000, 1, 256, new ResourceWeights(2.0f)),
        createSchedulable(1000, 2, 256)) < 0);
  }
  
  @Test
  public void testUnevenWeightsSameDominantResource() {
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(3000, 1, 256, new ResourceWeights(2.0f, 1.0f)),
        createSchedulable(2000, 1, 256)) < 0);
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(1000, 3, 256, new ResourceWeights(1.0f, 2.0f)),
        createSchedulable(1000, 2, 256)) < 0);
  }
  
  @Test
  public void testUnevenWeightsDifferentDominantResource() {
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(1000, 3, 256, new ResourceWeights(1.0f, 2.0f)),
        createSchedulable(2000, 1, 256)) < 0);
    assertTrue(createComparator(8000, 8, 256).compare(
        createSchedulable(3000, 1, 256, new ResourceWeights(2.0f, 1.0f)),
        createSchedulable(1000, 2, 256)) < 0);
  }
  
  @Test
  public void testCalculateShares() {
    Resource used = Resources.createResource(10, 5, 5);
    Resource capacity = Resources.createResource(100, 10, 10);
    ResourceType[] resourceOrder = new ResourceType[2];
    ResourceWeights shares = new ResourceWeights();
    DominantResourceFairnessPolicy.DominantResourceFairnessComparator comparator =
        new DominantResourceFairnessPolicy.DominantResourceFairnessComparator();
    comparator.calculateShares(used, capacity, shares, resourceOrder,
        ResourceWeights.NEUTRAL);
    
    assertEquals(.1, shares.getWeight(ResourceType.MEMORY), .00001);
    assertEquals(.5, shares.getWeight(ResourceType.CPU), .00001);
    assertEquals(ResourceType.CPU, resourceOrder[0]);
    assertEquals(ResourceType.MEMORY, resourceOrder[1]);
  }
}
