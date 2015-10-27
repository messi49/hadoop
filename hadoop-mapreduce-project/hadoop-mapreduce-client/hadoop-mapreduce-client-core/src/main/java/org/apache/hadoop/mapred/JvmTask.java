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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * Task abstraction that can be serialized, implements the writable interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JvmTask implements Writable {
  Task t;
  int gpuDeviceId;
  boolean shouldDie;
  public JvmTask(Task t, boolean shouldDie) {
    this.t = t;
    this.shouldDie = shouldDie;
  }
  public JvmTask(Task t, int gpuDeviceId, boolean shouldDie) {
    this.t = t;
    this.gpuDeviceId = gpuDeviceId;
    this.shouldDie = shouldDie;
  }
  public JvmTask() {}
  public Task getTask() {
    return t;
  }
  public int getGpuDeviceID() {
    return gpuDeviceId;
  }
  public boolean shouldDie() {
    return shouldDie;
  }
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(shouldDie);
    out.writeInt(gpuDeviceId);
    if (t != null) {
      out.writeBoolean(true);
      out.writeBoolean(t.isMapTask());
      t.write(out);
    } else {
      out.writeBoolean(false);
    }
  }
  public void readFields(DataInput in) throws IOException {
    shouldDie = in.readBoolean();
    gpuDeviceId = in.readInt();
    boolean taskComing = in.readBoolean();
    if (taskComing) {
      boolean isMap = in.readBoolean();
      if (isMap) {
        t = new MapTask();
      } else {
        t = new ReduceTask();
      }
      t.readFields(in);
    }
  }
}
