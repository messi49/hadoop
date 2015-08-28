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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.GpuStatus;
import org.apache.hadoop.yarn.proto.YarnProtos.GpuStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.GpuStatusProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class GpuStatusPBImpl extends GpuStatus {
  GpuStatusProto proto = GpuStatusProto.getDefaultInstance();
  GpuStatusProto.Builder builder = null;
  boolean viaProto = false;

  public GpuStatusPBImpl() {
    builder = GpuStatusProto.newBuilder();
  }

  public GpuStatusPBImpl(GpuStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized GpuStatusProto getProto() {
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
    sb.append("GPU Status: [");
    sb.append("DeviceID: ").append(getDeviceId()).append(", ");
    sb.append("GPU Utilization: ").append(getGpuUtilization()).append(", ");
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
      builder = GpuStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized int getDeviceId() {
    GpuStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDeviceId();
  }

  @Override
  public synchronized void setDeviceId(int deviceId) {
    maybeInitBuilder();
    builder.setDeviceId(deviceId);
  }

  @Override
  public synchronized int getGpuUtilization() {
    GpuStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getGpuUtilization();
  }

  @Override
  public synchronized void setGpuUtilization(int gpuUtilization) {
    maybeInitBuilder();
    builder.setGpuUtilization(gpuUtilization);
  }

}