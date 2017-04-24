/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package clustering.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Created by edwardlol on 17-4-21.
 */
public final class MapReduceUtils {
    //~ Constructors -----------------------------------------------------------

    private MapReduceUtils() {
    }

    //~ Methods ----------------------------------------------------------------

    public static Configuration initConf(Configuration conf) {
        if (conf == null) {
            conf = new Configuration();
            // TODO: 17-4-24 is it possible to auto set user name?
            conf.set("fs.defaultFS", "hdfs://localhost:9000/user/edwardlol");
        } else {
            conf.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
        }
        return conf;
    }

    public static void runJobs(JobControl jobControl) {
        Thread jobRunnerThread = new Thread(jobControl);
        long starttime = System.currentTimeMillis();
        jobRunnerThread.start();

        while (!jobControl.allFinished()) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endtime = System.currentTimeMillis();
        System.out.println("All jobs finished in: " + (endtime - starttime) / 1000 + " seconds");

        jobControl.stop();
    }
}

// End MapReduceUtils.java
