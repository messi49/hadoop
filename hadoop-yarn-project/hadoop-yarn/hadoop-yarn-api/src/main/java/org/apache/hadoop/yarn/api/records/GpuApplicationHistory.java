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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>GpuApplicationHisory</code> represents the GPU Application Utilization.</p>
 *
 * <p>It provides details such as:
 *   <ul>
 *     <li><em>DeviceID</em> of the device.</li>
 *     <li><em>GPU Utilization</em> of the GPU.</li>
 *     <li><em>TaskType</em> of the Map or Reduce.</li>
 *   </ul>
 * </p>
 */
@Public
@Stable
public abstract class GpuApplicationHistory {

  @Private
  @Unstable
  public static GpuApplicationHistory newInstance(int deviceId, ApplicationId applicationId, int gpuExecutionTime, int taskType) {
    GpuApplicationHistory gpuApplicationHistory = Records.newRecord(GpuApplicationHistory.class);
    gpuApplicationHistory.setDeviceId(deviceId);
    gpuApplicationHistory.setApplicationId(applicationId);
    gpuApplicationHistory.setGpuExecutionTime(gpuExecutionTime);
    gpuApplicationHistory.setTaskType(taskType);
    return gpuApplicationHistory;
  }

  /**
   * Get the <em>DeviceID</em> of the GPU.
   * @return <em>DeviceID</em> of the GPU
   */
  @Public
  @Stable
  public abstract int getDeviceId();

  @Private
  @Unstable
  public abstract void setDeviceId(int deviceId);

  /**
   * Get the <em>ApplicationID</em> of the GPU.
   * @return <em>ApplicationID</em> of the GPU
   */
  @Public
  @Stable
  public abstract ApplicationId getApplicationId();

  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);

  /**
   * Get the <em>GPU Utilization</em> of the GPU.
   * @return <em>GPU Utilization</em> of the GPU
   */
  @Public
  @Stable
  public abstract int getGpuExecutionTime();

  @Private
  @Unstable
  public abstract void setGpuExecutionTime(int gpuExecutionTime);


  /**
   * Get the <em>TaskType</em> of the GPU.
   * @return <em>TaskType</em> of the GPU
   */
  @Public
  @Stable
  public abstract int getTaskType();

  @Private
  @Unstable
  public abstract void setTaskType(int taskType);
}
