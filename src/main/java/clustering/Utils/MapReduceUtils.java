package clustering.Utils;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Created by edwardlol on 17-4-21.
 */
public final class MapReduceUtils {
    //~ Constructors -----------------------------------------------------------

    private MapReduceUtils() {
    }

    //~ Methods ----------------------------------------------------------------

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
