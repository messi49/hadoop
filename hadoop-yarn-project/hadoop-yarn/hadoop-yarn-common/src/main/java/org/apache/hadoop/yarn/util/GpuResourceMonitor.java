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
  // key = pid, value = gpu memory
  static HashMap<String, Long> map = new HashMap<String, Long>();

  @Override
  public void run() {
    String command = "nvidia-smi";
    Process process = null;
    String line = null;

    Pattern gpuMemoryUsagePattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)((G|C\\+G))([\\x20\\t]+)([0-9a-zA-Z_\\-\\.\\/\\=(  )]+)([\\x20\\t]+)([\\d]+)(MiB)( |)");

    try {
      process = Runtime.getRuntime().exec(command);

      InputStream is = process.getInputStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(is));

      clearMap();

      while ((line = br.readLine()) != null) {
        Matcher memMatcher = gpuMemoryUsagePattern.matcher(line);

        //Get Memory Usage
        if (memMatcher.find()) {
          //System.out.println(memMatcher.group(3) + ": " + memMatcher.group(5)+ ": " + memMatcher.group(7)+ ": " + memMatcher.group(10)+ ": " + memMatcher.group(12));
          if (memMatcher.group(5).length() != 0 && Long.parseLong(memMatcher.group(12)) >= 0) {
            setGpuMemory(memMatcher.group(5), Long.parseLong(memMatcher.group(12)));
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  static synchronized void clearMap(){
    LOG.info("clearMap");
    GpuResourceMonitor.map.clear();
  }

  static synchronized void setGpuMemory(String pid, long gpuMemory){
    GpuResourceMonitor.map.put(pid, gpuMemory);
    LOG.info("setGpuMemory PID = " + pid + "GPU Memory = " + gpuMemory);
  }

  static synchronized long getGpuMemory(String pid){
    if (!GpuResourceMonitor.map.isEmpty() && GpuResourceMonitor.map.size() > 0) {
      LOG.info("getGpuMemory PID = " + pid + ", map size = " + GpuResourceMonitor.map.size());

      if(GpuResourceMonitor.map.containsKey(pid) == true)
        return GpuResourceMonitor.map.get(pid);
      else
        return 0;
    }
    else{
      return 0;
    }
  }
}