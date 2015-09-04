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

package org.apache.hadoop.yarn.api.records.impl.pb;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.GpuApplicationHistory;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.GpuApplicationHistoryProto;
import org.apache.hadoop.yarn.proto.YarnProtos.GpuApplicationHistoryProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GpuApplicationHistoryPBImpl extends GpuApplicationHistory {
  GpuApplicationHistoryProto proto = GpuApplicationHistoryProto.getDefaultInstance();
  GpuApplicationHistoryProto.Builder builder = null;
  private ApplicationId applicationId = null;
  boolean viaProto = false;

  public GpuApplicationHistoryPBImpl() {
    builder = GpuApplicationHistoryProto.newBuilder();
  }

  public GpuApplicationHistoryPBImpl(GpuApplicationHistoryProto proto) {
    this.proto = proto;
    viaProto = true;
    this.applicationId = convertFromProtoFormat(proto.getApplicationId());
  }

  public synchronized GpuApplicationHistoryProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GPU Application: [");
    sb.append("DeviceID: ").append(getDeviceId()).append(", ");
    sb.append("ApplicationID: ").append(getApplicationId()).append(", ");
    sb.append("GPU Utilization: ").append(getGpuUtilization()).append(", ");
    sb.append("Task Type: ").append(getTaskType()).append(", ");
    sb.append("]");
    return sb.toString();
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GpuApplicationHistoryProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized int getDeviceId() {
    GpuApplicationHistoryProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDeviceId();
  }

  @Override
  public synchronized void setDeviceId(int deviceId) {
    maybeInitBuilder();
    builder.setDeviceId(deviceId);
  }

  @Override
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId appId) {
    if (appId != null) {
      Preconditions.checkNotNull(builder);
      builder.setApplicationId(convertToProtoFormat(appId));
    }
    this.applicationId = appId;
  }

  @Override
  public synchronized int getGpuUtilization() {
    GpuApplicationHistoryProtoOrBuilder p = viaProto ? proto : builder;
    return p.getGpuUtilization();
  }

  @Override
  public synchronized void setGpuUtilization(int gpuUtilization) {
    maybeInitBuilder();
    builder.setGpuUtilization(gpuUtilization);
  }

  @Override
  public int getTaskType() {
    GpuApplicationHistoryProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTaskType();
  }

  @Override
  public void setTaskType(int taskType) {
    maybeInitBuilder();
    builder.setTaskType(taskType);
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
}
