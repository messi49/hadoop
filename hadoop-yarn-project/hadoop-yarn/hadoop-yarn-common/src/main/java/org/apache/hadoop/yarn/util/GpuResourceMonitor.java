package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gpu on 15/06/12.
 * monitor for gpu resource
 */
public class GpuResourceMonitor extends TimerTask {
  static final Log LOG = LogFactory.getLog(GpuResourceMonitor.class);

  // patterns of gpu process memory, utilization, gpu memory
  Pattern gpuProcessMemoryUsagePattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)((G|C\\+G))([\\x20\\t]+)([0-9a-zA-Z_\\-\\.\\/\\=(  )]+)([\\x20\\t]+)([\\d]+)(MiB)( |)");
  Pattern gpuMemoryUsagePattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)(MiB)([\\x20\\t]+)(/)([\\x20\\t]+)([\\d]+)(MiB)(|)");
  Pattern gpuUtilizationPattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)(%)([\\x20\\t]+)(Default)([\\x20\\t]+)(|)");

  // key = pid, value = gpu memory
  static HashMap<String, Long> gpuProcessMemoryUsage = new HashMap<String, Long>();
  //GPU Memory Usage
  static HashMap<Integer, Long> gpuMemoryUsage = new HashMap<Integer, Long>();
  // GPU Memory Size
  static HashMap<Integer, Long> gpuMemorySize = new HashMap<Integer, Long>();
  //GPU Memory Usage
  static HashMap<Integer, Integer> gpuUtilization = new HashMap<Integer, Integer>();

  boolean initFlag = false;

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


        //Get Process Memory Usage
        if (processMemMatcher.find()) {
          //System.out.println(memMatcher.group(3) + ": " + memMatcher.group(5)+ ": " + memMatcher.group(7)+ ": " + memMatcher.group(10)+ ": " + memMatcher.group(12));
          if (processMemMatcher.group(5).length() != 0 && Long.parseLong(processMemMatcher.group(12)) >= 0) {
            setGpuProcessMemory(processMemMatcher.group(5), Long.parseLong(processMemMatcher.group(12)));
          }
        }

        //Get Memory Usage
        if (MemMatcher.find()){
          //LOG.info("GPU Memory Usage(Device " + memCounter + "): " + MemMatcher.group(3) + " / " + MemMatcher.group(8));
          //Set GPU Memory Size
          if(initFlag == false){
            if(gpuMemorySize.containsKey(memCounter) == true){
              initFlag = true;
            }
            gpuMemorySize.put(memCounter, Long.parseLong(MemMatcher.group(3)));
          }

          setGpuMemoryUsage(memCounter, Long.parseLong(MemMatcher.group(3)));
          memCounter++;
        }

        //Get Utilization
        if (utilMatcher.find()){
          //LOG.info("GPU Util(Device " + utilCounter + "): " + utilMatcher.group(3) + "%");
          setGpuUtilization(utilCounter, Integer.parseInt(utilMatcher.group(3)));
          utilCounter++;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  static synchronized void clearMap(){
    //LOG.info("clearMap");
    GpuResourceMonitor.gpuProcessMemoryUsage.clear();
    GpuResourceMonitor.gpuMemoryUsage.clear();
    GpuResourceMonitor.gpuUtilization.clear();
  }

  static synchronized void setGpuProcessMemory(String pid, long gpuMemory){
    GpuResourceMonitor.gpuProcessMemoryUsage.put(pid, gpuMemory);
    //LOG.info("setGpuProcessMemory PID = " + pid + "GPU Memory = " + gpuMemory);
  }

  static synchronized long getGpuProcessMemory(String pid){
    if (!GpuResourceMonitor.gpuProcessMemoryUsage.isEmpty() && GpuResourceMonitor.gpuProcessMemoryUsage.size() > 0) {
      //LOG.info("getGpuProcessMemory PID = " + pid + ", map size = " + GpuResourceMonitor.gpuProcessMemoryUsage.size());

      if(GpuResourceMonitor.gpuProcessMemoryUsage.containsKey(pid) == true)
        return GpuResourceMonitor.gpuProcessMemoryUsage.get(pid);
      else
        return 0;
    }
    else{
      return 0;
    }
  }

  static synchronized void setGpuMemoryUsage(int deviceId, long gpuMemory){
    GpuResourceMonitor.gpuMemoryUsage.put(deviceId, gpuMemory);
    //LOG.info("GPU Memory Usage(Device " + deviceId + "): " + gpuMemory);
  }

  static synchronized void setGpuUtilization(int deviceId, int utilization){
    GpuResourceMonitor.gpuUtilization.put(deviceId, utilization);
    //LOG.info("GPU Util(Device " + deviceId + "): " + utilization + "%");
  }
}