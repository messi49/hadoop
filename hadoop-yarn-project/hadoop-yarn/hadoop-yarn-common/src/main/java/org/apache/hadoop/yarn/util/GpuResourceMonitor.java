package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gpu on 15/06/12.
 * monitor for gpu resource
 */
public class GpuResourceMonitor{
  static final Log LOG = LogFactory
    .getLog(GpuResourceMonitor.class);

  // key = pid, value = gpu memory
  static HashMap<String,Long> map = new HashMap<String,Long>();
  static getGpuResource monitorThread = new getGpuResource();
  static boolean run_flag = true;

  public void GpuResourceMonitor(){
    monitorThread.run();
  };

  public void stopMonitorThread(){
    run_flag = false;
  }

  private static class getGpuResource extends Thread {
    public void run() {
      String command = "nvidia-smi";
      Process process = null;
      String line = null;

      Pattern gpuMemoryUsagePattern = Pattern.compile("(|)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)([\\d]+)([\\x20\\t]+)((G|C\\+G))([\\x20\\t]+)([0-9a-zA-Z_\\-\\.\\/\\=(  )]+)([\\x20\\t]+)([\\d]+)(MiB)( |)");

      while (run_flag) {
        try {
          process = Runtime.getRuntime().exec(command);

          InputStream is = process.getInputStream();
          BufferedReader br = new BufferedReader(new InputStreamReader(is));

          map.clear();

          while ((line = br.readLine()) != null) {
            Matcher memMatcher = gpuMemoryUsagePattern.matcher(line);

            //Get Memory Usage
            if (memMatcher.find()) {
              //System.out.println(memMatcher.group(3) + ": " + memMatcher.group(5)+ ": " + memMatcher.group(7)+ ": " + memMatcher.group(10)+ ": " + memMatcher.group(12));
              if (memMatcher.group(5).length() != 0 && Long.parseLong(memMatcher.group(12)) >= 0) {
                map.put(memMatcher.group(5), Long.parseLong(memMatcher.group(12)));
                LOG.info("gpuResourceMonitor pid = " + memMatcher.group(5) + "GPU Memory = " + Long.parseLong(memMatcher.group(12)));
              }
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
  static long getGpuMemory(String pid){
    long gpuMemoryUsage = 0;

    if (!map.isEmpty() && map.size() > 0) {
      gpuMemoryUsage = map.get(pid);

      if(gpuMemoryUsage >= 0)
        return gpuMemoryUsage;
      else
        return 0;
    }
    else{
      return 0;
    }
  }
}