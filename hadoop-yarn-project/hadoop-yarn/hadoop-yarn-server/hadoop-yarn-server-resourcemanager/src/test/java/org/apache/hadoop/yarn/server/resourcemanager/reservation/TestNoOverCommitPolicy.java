/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.MismatchedUserException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;
import org.junit.Test;

public class TestNoOverCommitPolicy {

  long step;
  long initTime;

  InMemoryPlan plan;
  ReservationAgent mAgent;
  Resource minAlloc;
  ResourceCalculator res;
  Resource maxAlloc;

  int totCont = 1000000;

  @Before
  public void setup() throws Exception {

    // 1 sec step
    step = 1000L;

    initTime = System.currentTimeMillis();
    minAlloc = Resource.newInstance(1024, 1, 256);
    res = new DefaultResourceCalculator();
    maxAlloc = Resource.newInstance(1024 * 8, 8, 256 * 4);

    mAgent = mock(ReservationAgent.class);
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    CapacityScheduler scheduler = testUtil.mockCapacityScheduler(totCont);
    String reservationQ = testUtil.getFullReservationQueueName();
    CapacitySchedulerConfiguration capConf = scheduler.getConfiguration();
    NoOverCommitPolicy policy = new NoOverCommitPolicy();
    policy.init(reservationQ, capConf);

    plan =
        new InMemoryPlan(scheduler.getRootQueueMetrics(), policy, mAgent,
            scheduler.getClusterResource(), step, res, minAlloc, maxAlloc,
            "dedicated", null, true);
  }

  public int[] generateData(int length, int val) {
    int[] data = new int[length];
    for (int i = 0; i < length; i++) {
      data[i] = val;
    }
    return data;
  }

  @Test
  public void testSingleUserEasyFitPass() throws IOException, PlanningException {
    // generate allocation that easily fit within resource constraints
    int[] f = generateData(3600, (int) Math.ceil(0.2 * totCont));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
  }

  @Test
  public void testSingleUserBarelyFitPass() throws IOException,
      PlanningException {
    // generate allocation from single tenant that barely fit
    int[] f = generateData(3600, totCont);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
  }

  @Test(expected = ResourceOverCommitException.class)
  public void testSingleFail() throws IOException, PlanningException {
    // generate allocation from single tenant that exceed capacity
    int[] f = generateData(3600, (int) (1.1 * totCont));
    plan.addReservation(new InMemoryReservationAllocation(
        ReservationSystemTestUtil.getNewReservationId(), null, "u1",
        "dedicated", initTime, initTime + f.length, ReservationSystemTestUtil
            .generateAllocation(initTime, step, f), res, minAlloc));
  }

  @Test(expected = MismatchedUserException.class)
  public void testUserMismatch() throws IOException, PlanningException {
    // generate allocation from single tenant that exceed capacity
    int[] f = generateData(3600, (int) (0.5 * totCont));

    ReservationId rid = ReservationSystemTestUtil.getNewReservationId();
    plan.addReservation(new InMemoryReservationAllocation(rid, null, "u1",
        "dedicated", initTime, initTime + f.length, ReservationSystemTestUtil
            .generateAllocation(initTime, step, f), res, minAlloc));

    // trying to update a reservation with a mismatching user
    plan.updateReservation(new InMemoryReservationAllocation(rid, null, "u2",
        "dedicated", initTime, initTime + f.length, ReservationSystemTestUtil
            .generateAllocation(initTime, step, f), res, minAlloc));
  }

  @Test
  public void testMultiTenantPass() throws IOException, PlanningException {
    // generate allocation from multiple tenants that barely fit in tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    for (int i = 0; i < 4; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
    }
  }

  @Test(expected = ResourceOverCommitException.class)
  public void testMultiTenantFail() throws IOException, PlanningException {
    // generate allocation from multiple tenants that exceed tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    for (int i = 0; i < 5; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
    }
  }
}
