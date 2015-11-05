package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import java.util.List;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gpu on 15/06/12.
 * monitor for gpu resource
 */
public class GpuResourceMonitor extends TimerTask {
  static final Log LOG = LogFactory.getLog(GpuResourceMonitor.class);

  public static final Object PROCESS_MEMORY_USAGE_LOCK = new Object();
  public static final Object MEMORY_USAGE_LOCK = new Object();
  public static final Object UTILIZATION_LOCK = new Object();
  public static final Object APP_UTILIZATION_LOCK = new Object();

  // patterns of gpu process memory, utilization, gpu memory
  Pattern gpuProcessMemoryUsagePattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)((G|C\\+G))([\\x20\\t]+)([0-9a-zA-Z_\\-\\.\\/\\=(  )]+)([\\x20\\t]+)([\\d]+)(MiB)( |)");
  Pattern gpuMemoryUsagePattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)(MiB)([\\x20\\t]+)(/)([\\x20\\t]+)([\\d]+)(MiB)(|)");
  Pattern gpuUtilizationPattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)(%)([\\x20\\t]+)(Default)([\\x20\\t]+)(|)");

  // key = pid, value = gpu memory
  static HashMap<String, Long> gpuProcessMemoryUsage = new HashMap<String, Long>();
  // GPU Device Memory Size
  static HashMap<Integer, Long> gpuDevicesMemory= new HashMap<Integer, Long>();
  // GPU Memory Usage
  static HashMap<Integer, Long> gpuMemoryUsage = new HashMap<Integer, Long>();
  // GPU Free Memory
  static HashMap<Integer, Long> gpuFreeMemory = new HashMap<Integer, Long>();
  // GPU Utilization
  static HashMap<Integer, Integer> gpuUtilization = new HashMap<Integer, Integer>();
  // App GPU Utilization
  static HashMap<Integer, ApplicationId> gpuAppList = new HashMap<Integer, ApplicationId>();
  static HashMap<ApplicationId, Integer> activeGpuAppList = new HashMap<ApplicationId, Integer>();
  static HashMap<ApplicationId, Integer> minGpuAppUtilization = new HashMap<ApplicationId, Integer>();
  static HashMap<ApplicationId, Integer> maxGpuAppUtilization = new HashMap<ApplicationId, Integer>();

  @Override
  public void run() {
    String command = "nvidia-smi";
    Process process = null;
    String line = null;
    int memCounter = 0, utilCounter = 0;

    try {
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

          // Add App GPU Utilization Log
          synchronized (APP_UTILIZATION_LOCK) {
            if (activeGpuAppList.size() > 0) {
              for (int i = 0; i < gpuAppList.size(); i++) {
                if(activeGpuAppList.containsKey(gpuAppList.get(i)) == true){
                  if(maxGpuAppUtilization.get(gpuAppList.get(i)) < gpuUtilization) {
                    maxGpuAppUtilization.put(gpuAppList.get(i), gpuUtilization);
                  }
                }
              }
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  static synchronized void clearMap() {
    //LOG.info("clearMap");
    synchronized (PROCESS_MEMORY_USAGE_LOCK) {
      gpuProcessMemoryUsage.clear();
    }
    synchronized (MEMORY_USAGE_LOCK) {
      gpuMemoryUsage.clear();
    }
    synchronized (UTILIZATION_LOCK) {
      gpuUtilization.clear();
    }
  }

  static void setGpuProcessMemoryUsage(String pid, long gpuMemory) {
    synchronized (gpuProcessMemoryUsage) {
      gpuProcessMemoryUsage.put(pid, gpuMemory);
      //LOG.info("setGpuProcessMemory PID = " + pid + "GPU Memory = " + gpuMemory);
    }
  }

  static long getGpuProcessMemoryUsage(String pid) {
    synchronized (gpuProcessMemoryUsage) {
      if (!gpuProcessMemoryUsage.isEmpty() && GpuResourceMonitor.gpuProcessMemoryUsage.size() > 0) {
        //LOG.info("getGpuProcessMemory PID = " + pid + ", map size = " + GpuResourceMonitor.gpuProcessMemoryUsage.size());
        if (gpuProcessMemoryUsage.containsKey(pid) == true)
          return gpuProcessMemoryUsage.get(pid);
        else
          return 0;
      } else {
        return 0;
      }
    }
  }

  static void setGpuDeviceMemory(int deviceId, long gpuMemory) {
    synchronized (MEMORY_USAGE_LOCK) {
      if(gpuDevicesMemory.containsKey(deviceId) == false){
        gpuDevicesMemory.put(deviceId, gpuMemory);
      }
    }
  }

  static void setGpuMemoryUsage(int deviceId, long gpuMemory) {
    synchronized (MEMORY_USAGE_LOCK) {
      gpuMemoryUsage.put(deviceId, gpuMemory);
      gpuFreeMemory.put(deviceId, gpuDevicesMemory.get(deviceId) - gpuMemory);
      //LOG.info("GPU Memory Unusage(Device " + deviceId + "): " + (gpuDevicesMemory.get(deviceId) - gpuMemory));
    }
  }

  static long getGpuMemoryUsage(int deviceId) {
    synchronized (MEMORY_USAGE_LOCK) {
      //LOG.info("GPU Memory Unusage(Device " + deviceId + "): " + gpuMemory);
      if (!gpuMemoryUsage.isEmpty() && GpuResourceMonitor.gpuMemoryUsage.size() > 0) {
        if (gpuMemoryUsage.containsKey(deviceId) == true) {
          return gpuMemoryUsage.get(deviceId);
        } else {
          return 0;
        }
      } else {
        return 0;
      }
    }
  }

  static HashMap<Integer, Long> getGpuFreeMemory() {
    synchronized (MEMORY_USAGE_LOCK) {
      if (!gpuFreeMemory.isEmpty() && GpuResourceMonitor.gpuFreeMemory.size() > 0) {
        return gpuFreeMemory;
      } else {
        return null;
      }
    }
  }

  static void setGpuUtilization(int deviceId, int utilization) {
    synchronized (UTILIZATION_LOCK) {
      gpuUtilization.put(deviceId, utilization);
      //LOG.info("GPU Util(Device " + deviceId + "): " + utilization + "%");
    }
  }

  static int getGpuUtilization(int deviceId) {
    synchronized (UTILIZATION_LOCK) {
      return gpuUtilization.get(deviceId);
    }
  }

  public static List<GpuStatus> getGpuStatuses() {
    List<GpuStatus> gpuStatuses = new ArrayList<GpuStatus>();

    synchronized (UTILIZATION_LOCK) {
      synchronized (MEMORY_USAGE_LOCK) {
        for (Integer deviceId : gpuUtilization.keySet()) {
          GpuStatusPBImpl gpuStatus = new GpuStatusPBImpl();

          gpuStatus.setDeviceId(deviceId);
          gpuStatus.setGpuUtilization(gpuUtilization.get(deviceId));
          gpuStatus.setGpuFreeMemory(gpuFreeMemory.get(deviceId).intValue());

          gpuStatuses.add(gpuStatus);
        }
      }
    }
    return gpuStatuses;
  }

  public static void startGpuUtilizationMonitor(int deviceId, ApplicationId appId, ContainerId containerId) {
    synchronized (APP_UTILIZATION_LOCK) {

      if (activeGpuAppList.containsKey(appId) == false) {
        // First, add GPU App ID
        if(gpuAppList.containsKey(appId) == false) {
          gpuAppList.put(gpuAppList.size(), appId);
        }
        activeGpuAppList.put(appId, 1);

        // Second, add start time GPU Util
        minGpuAppUtilization.put(appId, getGpuUtilization(deviceId));
        maxGpuAppUtilization.put(appId, getGpuUtilization(deviceId));
      } else {
        // count GPU App
        activeGpuAppList.put(appId, activeGpuAppList.get(appId) + 1);
      }
    }
  }

  public static void removeGpuUtilizationMonitor(int deviceId, ApplicationId appId, ContainerId containerId) {
    synchronized (APP_UTILIZATION_LOCK) {
      if(activeGpuAppList.containsKey(appId) == true){
        activeGpuAppList.remove(appId);
      }
    }
  }

  public static int getGpuAppUtilization(int deviceId, ApplicationId appId) {
    //LOG.info("getGpuAppUtilization: min = " + minGpuAppUtilization.get(appId) + ", max = " + maxGpuAppUtilization.get(appId) + ", active = " + activeGpuAppList.get(appId) +
    //", Ans = " + ((maxGpuAppUtilization.get(appId) - minGpuAppUtilization.get(appId)) / activeGpuAppList.get(appId)));

    if(activeGpuAppList.containsKey(appId) == true) {
      return (maxGpuAppUtilization.get(appId) - minGpuAppUtilization.get(appId)) / activeGpuAppList.get(appId);
    }
    else{
      return maxGpuAppUtilization.get(appId) - minGpuAppUtilization.get(appId);
    }
  }


  public static List<GpuApplicationHistory> getGpuApplicationHistory() {
    List<GpuApplicationHistory> gpuApplicationHistories = new ArrayList<GpuApplicationHistory>();

    synchronized (APP_UTILIZATION_LOCK) {
      if(activeGpuAppList.size() > 0) {
        for (int i = 0; i < gpuAppList.size(); i++) {
          GpuApplicationHistoryPBImpl gpuApplicationHistory = new GpuApplicationHistoryPBImpl();
          gpuApplicationHistory.setDeviceId(0);
          gpuApplicationHistory.setApplicationId(gpuAppList.get(i));
          gpuApplicationHistory.setGpuUtilization(getGpuAppUtilization(0, gpuAppList.get(i)));
          gpuApplicationHistory.setTaskType(0);

          gpuApplicationHistories.add(gpuApplicationHistory);
        }
      }
    }

    return gpuApplicationHistories;
  }
}