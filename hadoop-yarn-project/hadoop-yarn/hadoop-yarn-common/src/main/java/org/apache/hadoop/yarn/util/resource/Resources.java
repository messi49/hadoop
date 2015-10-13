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

package org.apache.hadoop.yarn.util.resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.GpuStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
@Unstable
public class Resources {
  private static final Log LOG = LogFactory.getLog(Resources.class);

  // Java doesn't have const :(
  private static final Resource NONE = new Resource() {

    @Override
    public int getMemory() {
      return 0;
    }

    @Override
    public void setMemory(int memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return 0;
    }

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getGpuMemory() { return 0; }

    @Override
    public void setGpuMemory(int gpuMemory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      int diff = 0 - o.getMemory();
      if (diff == 0) {
        diff = 0 - o.getVirtualCores();
        if (diff == 0) {
          diff = 0 - o.getGpuMemory();
        }
      }
      return diff;
    }

  };
  
  private static final Resource UNBOUNDED = new Resource() {

    @Override
    public int getMemory() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setMemory(int memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getGpuMemory() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setGpuMemory(int gpuMemory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      int diff = 0 - o.getMemory();
      if (diff == 0) {
        diff = 0 - o.getVirtualCores();
        if (diff == 0) {
          diff = 0 - o.getGpuMemory();
        }
      }
      return diff;
    }
    
  };

  public static Resource createResource(int memory, int gpuMemory) {
    return createResource(memory, (memory > 0) ? 1 : 0, gpuMemory);
  }

//  public static Resource createResource(int memory, int gpuMemory) {
//    return createResource(memory, (memory > 0) ? 1 : 0, gpuMemory);
//  }

  public static Resource createResource(int memory, int cores, int gpuMemory) {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    resource.setVirtualCores(cores);
    resource.setGpuMemory(gpuMemory);
    return resource;
  }

  public static Resource none() {
    return NONE;
  }
  
  public static Resource unbounded() {
    return UNBOUNDED;
  }

  public static Resource clone(Resource res) {
    return createResource(res.getMemory(), res.getVirtualCores(), res.getGpuMemory());
  }

  public static Resource addTo(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() + rhs.getMemory());
    lhs.setVirtualCores(lhs.getVirtualCores() + rhs.getVirtualCores());
    lhs.setGpuMemory(lhs.getGpuMemory() + rhs.getGpuMemory());
    return lhs;
  }

  public static Resource add(Resource lhs, Resource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static Resource subtractFrom(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() - rhs.getMemory());
    lhs.setVirtualCores(lhs.getVirtualCores() - rhs.getVirtualCores());
    lhs.setGpuMemory(lhs.getGpuMemory() - rhs.getGpuMemory());
    return lhs;
  }

  public static Resource subtract(Resource lhs, Resource rhs) {
    return subtractFrom(clone(lhs), rhs);
  }

  public static Resource negate(Resource resource) {
    return subtract(NONE, resource);
  }

  public static Resource multiplyTo(Resource lhs, double by) {
    lhs.setMemory((int)(lhs.getMemory() * by));
    lhs.setVirtualCores((int) (lhs.getVirtualCores() * by));
    lhs.setGpuMemory((int) (lhs.getGpuMemory() * by));
    return lhs;
  }

  public static Resource multiply(Resource lhs, double by) {
    return multiplyTo(clone(lhs), by);
  }
  
  public static Resource multiplyAndNormalizeUp(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeUp(lhs, by, factor);
  }
  
  public static Resource multiplyAndNormalizeDown(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeDown(lhs, by, factor);
  }
  
  public static Resource multiplyAndRoundDown(Resource lhs, double by) {
    Resource out = clone(lhs);
    out.setMemory((int)(lhs.getMemory() * by));
    out.setVirtualCores((int) (lhs.getVirtualCores() * by));
    out.setGpuMemory((int) (lhs.getGpuMemory() * by));
    return out;
  }
  
  public static Resource normalize(
      ResourceCalculator calculator, Resource lhs, Resource min,
      Resource max, Resource increment) {
    return calculator.normalize(lhs, min, max, increment);
  }
  
  public static Resource roundUp(
      ResourceCalculator calculator, Resource lhs, Resource factor) {
    return calculator.roundUp(lhs, factor);
  }
  
  public static Resource roundDown(
      ResourceCalculator calculator, Resource lhs, Resource factor) {
    return calculator.roundDown(lhs, factor);
  }
  
  public static boolean isInvalidDivisor(
      ResourceCalculator resourceCalculator, Resource divisor) {
    return resourceCalculator.isInvalidDivisor(divisor);
  }

  public static float ratio(
      ResourceCalculator resourceCalculator, Resource lhs, Resource rhs) {
    return resourceCalculator.ratio(lhs, rhs);
  }
  
  public static float divide(
      ResourceCalculator resourceCalculator,
      Resource clusterResource, Resource lhs, Resource rhs) {
    return resourceCalculator.divide(clusterResource, lhs, rhs);
  }
  
  public static Resource divideAndCeil(
      ResourceCalculator resourceCalculator, Resource lhs, int rhs) {
    return resourceCalculator.divideAndCeil(lhs, rhs);
  }
  
  public static boolean equals(Resource lhs, Resource rhs) {
    return lhs.equals(rhs);
  }

  public static boolean lessThan(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) < 0);
  }

  public static boolean lessThanOrEqual(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) <= 0);
  }

  public static boolean greaterThan(
      ResourceCalculator resourceCalculator,
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) > 0;
  }

  public static boolean greaterThanOrEqual(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0;
  }
  
  public static Resource min(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) <= 0 ? lhs : rhs;
  }

  public static Resource max(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0 ? lhs : rhs;
  }
  
  public static boolean fitsIn(Resource smaller, Resource bigger) {
    return smaller.getMemory() <= bigger.getMemory() &&
        smaller.getVirtualCores() <= bigger.getVirtualCores() &&
        smaller.getGpuMemory() <= bigger.getGpuMemory();
  }
  
  public static Resource componentwiseMin(Resource lhs, Resource rhs) {
    return createResource(Math.min(lhs.getMemory(), rhs.getMemory()),
        Math.min(lhs.getVirtualCores(), rhs.getVirtualCores()),
            Math.min(lhs.getGpuMemory(), rhs.getGpuMemory()));
  }

  public static int minGpuUtilization(List<GpuStatus> gpuStatuses) {
    int min = 100;
    for(int i = 0; i < gpuStatuses.size(); i++){
      if(min > gpuStatuses.get(i).getGpuUtilization()){
        min = gpuStatuses.get(i).getGpuUtilization();
      }
    }
    return min;
  }

  public static int getUseGpuId(List<GpuStatus> gpuStatuses, int gpuMemory) {
    int min = 100;
    int id = 0;
    for(int i = 0; i < gpuStatuses.size(); i++){
      if(min > gpuStatuses.get(i).getGpuUtilization() && gpuStatuses.get(i).getGpuFreeMemory() >= gpuMemory){
        min = gpuStatuses.get(i).getGpuUtilization();
        id = i;
      }
    }
    return id;
  }
}
