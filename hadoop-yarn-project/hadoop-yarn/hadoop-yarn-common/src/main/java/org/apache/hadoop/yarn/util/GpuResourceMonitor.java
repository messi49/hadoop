package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.GpuApplicationHistory;
import org.apache.hadoop.yarn.api.records.GpuStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.GpuApplicationHistoryPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.GpuStatusPBImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gpu on 15/06/12.
 * monitor for gpu resource
 */
public class GpuResourceMonitor extends Thread {
  static final Log LOG = LogFactory.getLog(GpuResourceMonitor.class);

  // patterns of gpu process memory, utilization, gpu memory
  Pattern gpuProcessMemoryUsagePattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)((C|G|C\\+G))([\\x20\\t]+)([0-9a-zA-Z_\\-\\.\\/\\=(  )]+)([\\x20\\t]+)([\\d]+)(MiB)( |)");
  Pattern gpuMemoryUsagePattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)(MiB)([\\x20\\t]+)(/)([\\x20\\t]+)([\\d]+)(MiB)(|)");
  Pattern gpuUtilizationPattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)(%)([\\x20\\t]+)(Default)([\\x20\\t]+)(|)");

  // GPU Memory Usage on process (key = pid, value = gpu memory)
  static HashMap<String, Long> gpuProcessMemoryUsage = new HashMap<String, Long>();
  // GPU Memory Usage's buffer
  static HashMap<String, Long> gpuProcessMemoryUsageBuf = new HashMap<String, Long>();
  // GPU Device Memory Size
  static HashMap<Integer, Long> gpuDevicesMemory= new HashMap<Integer, Long>();
  // GPU Memory Usage
  static HashMap<Integer, Long> gpuMemoryUsage = new HashMap<Integer, Long>();
  // GPU Free Memory
  static HashMap<Integer, Long> gpuFreeMemory = new HashMap<Integer, Long>();
  // GPU Utilization
  static HashMap<Integer, Integer> gpuUtilization = new HashMap<Integer, Integer>();
  // GPU Execution Time
  // <ApplicationId, Execution Time>
  static HashMap<ApplicationId, Integer> applicationGpuExecutionTime = new HashMap<ApplicationId, Integer>();
  // <ContainerId. ProcessId>
  static HashMap<ContainerId, String> monitorContainers = new HashMap<ContainerId, String>();
  // <ProcessId, Execution Time>
  static HashMap<String, Integer> processExecutionTimes = new HashMap<String, Integer>();

  public GpuResourceMonitor() {
    super("Gpu Resource Monitor");
  }

  @Override
  public void run() {
    String command = "nvidia-smi";
    Process process = null;
    String line = null;
    while(true) {
      try {
        int memCounter = 0, utilCounter = 0;

        process = Runtime.getRuntime().exec(command);

        InputStream is = process.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        clearMap();

        while ((line = br.readLine()) != null) {
          Matcher processMemMatcher = gpuProcessMemoryUsagePattern.matcher(line);
          Matcher MemMatcher = gpuMemoryUsagePattern.matcher(line);
          Matcher utilMatcher = gpuUtilizationPattern.matcher(line);

          // Get Process Memory Usage
          if (processMemMatcher.find()) {
            // System.out.println(memMatcher.group(3) + ": " + memMatcher.group(5)+ ": " + memMatcher.group(7)+ ": " + memMatcher.group(10)+ ": " + memMatcher.group(12));
            if (processMemMatcher.group(5).length() != 0 && Long.parseLong(processMemMatcher.group(12)) >= 0) {
              setGpuProcessMemoryUsage(processMemMatcher.group(5), Long.parseLong(processMemMatcher.group(12)));
            }
          }

          // Get Memory Usage
          if (MemMatcher.find()) {
            // Set GPU Device Memory (only first time)
            setGpuDeviceMemory(memCounter, Long.parseLong(MemMatcher.group(8)));
            // Set GPU Memory Usage
            setGpuMemoryUsage(memCounter, Long.parseLong(MemMatcher.group(3)));
            memCounter++;
          }

          //Get Utilization
          if (utilMatcher.find()) {
            //LOG.info("GPU Util(Device " + utilCounter + "): " + utilMatcher.group(3) + "%");
            int gpuUtilization = Integer.parseInt(utilMatcher.group(3));
            setGpuUtilization(utilCounter, gpuUtilization);
            utilCounter++;
          }

        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        LOG.warn(GpuResourceMonitor.class.getName()
          + " is interrupted. Exiting.");
        break;
      }
    }
  }

  static synchronized void clearMap() {
    //LOG.info("clearMap");
    gpuProcessMemoryUsage.clear();

    // DeepCopy
    Set keySet = gpuProcessMemoryUsageBuf.keySet();
    Iterator iteKey = keySet.iterator();
    while(iteKey.hasNext()) {
      String key = String.valueOf(iteKey.next());
      gpuProcessMemoryUsage.put(key, gpuProcessMemoryUsageBuf.get(key));
    }
  }

  static synchronized void setGpuProcessMemoryUsage(String pid, long gpuMemory) {
    gpuProcessMemoryUsageBuf.put(pid, gpuMemory);
    // Count up for GPU Execution Time
    setProcessGpuExecutionTime(pid);

    ArrayList<String> ppid = getPpid(pid);
    for (int i = 0; i < ppid.size(); i++){
      gpuProcessMemoryUsageBuf.put(ppid.get(i), gpuMemory);
      setProcessGpuExecutionTime(ppid.get(i));
    }
  }

  private static ArrayList<String> getPpid(String pid) {
    String[] command =  {"/bin/bash", "-c", "cat /proc/" + pid + "/stat|awk '{print $4}'"} ;
    Process process = null;
    String line = null;

    ArrayList<String> ppid = new ArrayList();
    try {
      process = Runtime.getRuntime().exec(command);

      InputStream is = process.getInputStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(is));

      while ((line = br.readLine()) != null) {
        ppid.add(line);
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    return ppid;
  }

  static synchronized long getGpuProcessMemoryUsage(String pid) {
    if (gpuProcessMemoryUsage.containsKey(pid) == true){
      return gpuProcessMemoryUsage.get(pid) * 1024 * 1024;
    }
    return 0;
  }

  private static ArrayList<String> getChildPid(String ppid) {
    String command = "pgrep -P " + ppid;
    Process process = null;
    String line = null;

    ArrayList<String> pid = new ArrayList();
    try {
      process = Runtime.getRuntime().exec(command);

      InputStream is = process.getInputStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(is));

      while ((line = br.readLine()) != null) {
        pid.add(line);
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    return pid;
  }

  static synchronized void setGpuDeviceMemory(int deviceId, long gpuMemory) {
    if(gpuDevicesMemory.containsKey(deviceId) == false){
      gpuDevicesMemory.put(deviceId, gpuMemory);
    }
  }

  static synchronized void setGpuMemoryUsage(int deviceId, long gpuMemory) {
    gpuMemoryUsage.put(deviceId, gpuMemory);
    gpuFreeMemory.put(deviceId, gpuDevicesMemory.get(deviceId) - gpuMemory);
    //LOG.info("GPU Memory Unusage(Device " + deviceId + "): " + (gpuDevicesMemory.get(deviceId) - gpuMemory));
  }

  static synchronized long getGpuMemoryUsage(int deviceId) {
    if (gpuMemoryUsage.containsKey(deviceId) == true) {
      return gpuMemoryUsage.get(deviceId);
    }
    return 0;
  }

  static synchronized HashMap<Integer, Long> getGpuFreeMemory() {
    return gpuFreeMemory;
  }

  static synchronized void setGpuUtilization(int deviceId, int utilization) {
    gpuUtilization.put(deviceId, utilization);
    //LOG.info("GPU Util(Device " + deviceId + "): " + utilization + "%");
  }

  static synchronized int getGpuUtilization(int deviceId) {
      return gpuUtilization.get(deviceId);
  }

  public synchronized static List<GpuStatus> getGpuStatuses() {
    List<GpuStatus> gpuStatuses = new ArrayList<GpuStatus>();

    for (Integer deviceId : gpuUtilization.keySet()) {
      GpuStatusPBImpl gpuStatus = new GpuStatusPBImpl();

      gpuStatus.setDeviceId(deviceId);
      gpuStatus.setGpuUtilization(gpuUtilization.get(deviceId));
      gpuStatus.setGpuFreeMemory(gpuFreeMemory.get(deviceId).intValue());

      gpuStatuses.add(gpuStatus);
    }

    return gpuStatuses;
  }

  public synchronized static void setMonitorContainerId(ContainerId containerId, String pid) {
    if(monitorContainers.containsKey(containerId) == false) {
      monitorContainers.put(containerId, pid);
    }
  }

  public synchronized static void removeMonitorContainerId(ContainerId containerId) {
    if(monitorContainers.containsValue(containerId)){
      // Remove Monitor Process
      processExecutionTimes.remove(monitorContainers.get(containerId));
      // Remove Monitor Container
      monitorContainers.remove(containerId);
    }
  }

  public synchronized static int setProcessGpuExecutionTime(String pid) {
    int executionTime = 1;

    if(processExecutionTimes.containsKey(pid)){
      // Count Up Execution time
      executionTime = processExecutionTimes.get(pid) + 1;
    }
    processExecutionTimes.put(pid, executionTime);

    return 0;
  }

  public synchronized static List<GpuApplicationHistory> getGpuApplicationHistory() {
    List<GpuApplicationHistory> gpuApplicationHistories = new ArrayList<GpuApplicationHistory>();

    // Set applicationGpuExecutionTime
    for (Map.Entry<ContainerId, String> entry : monitorContainers.entrySet()) {
      ContainerId containerid = entry.getKey();
      String pid = entry.getValue();

      int executionTime = 0;
      ApplicationId appId = containerid.getApplicationAttemptId().getApplicationId();

      // check monitor process id
      if (processExecutionTimes.containsKey(pid)) {
        executionTime = processExecutionTimes.get(pid);

        if (applicationGpuExecutionTime.containsKey(appId)) {
          if (applicationGpuExecutionTime.get(appId) < executionTime) {
            applicationGpuExecutionTime.put(appId, executionTime);
          }
        } else {
          applicationGpuExecutionTime.put(appId, executionTime);
        }
      }
    }

    // Set gpuApplicationHistories
    for (Map.Entry<ApplicationId, Integer> entry : applicationGpuExecutionTime.entrySet()) {
      ApplicationId appId = entry.getKey();
      int executionTime = entry.getValue();
      GpuApplicationHistoryPBImpl gpuApplicationHistory = new GpuApplicationHistoryPBImpl();
      gpuApplicationHistory.setDeviceId(0);
      gpuApplicationHistory.setApplicationId(appId);
      gpuApplicationHistory.setGpuExecutionTime(executionTime);
      gpuApplicationHistory.setTaskType(0);

      gpuApplicationHistories.add(gpuApplicationHistory);
    }

    return gpuApplicationHistories;
  }
}