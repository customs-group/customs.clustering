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
package clustering.tf_idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static clustering.Utils.MapReduceUtils.initConf;
import static clustering.Utils.MapReduceUtils.runJobs;

/**
 * Calculate the tf-idf of every term in each document.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class Driver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s simhash_result_dir output_dir " +
                    "[gname_weight]\n", getClass().getSimpleName());
            System.exit(1);
        }

        String docCntDir = args[1] + "/docCnt";
        String step1_outputDir = args[1] + "/step1";
        String step2_outputDir = args[1] + "/step2";
        String step3_outputDir = args[1] + "/result";

        Configuration conf = getConf();
        conf = initConf(conf);

        JobControl jobControl = new JobControl("tf-idf jobs");

        /* pre step, count documents number in the corpus */
        DocCntDriver docCntDriver = new DocCntDriver();
        String[] preJobArgs = new String[2];
        preJobArgs[0] = args[0];
        preJobArgs[1] = docCntDir;

        Job preJob = docCntDriver.configJob(preJobArgs);

        ControlledJob controlledPreJob = new ControlledJob(conf);
        controlledPreJob.setJob(preJob);
        jobControl.addJob(controlledPreJob);

        /* step 1, calculate term count of each document */
        TermCntDriver termCntDriver = new TermCntDriver();
        String[] job1Args = new String[2];
        job1Args[0] = args[0];
        job1Args[1] = step1_outputDir;
        Job job1 = termCntDriver.configJob(job1Args);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        /* step 2, calculate the term frequency of each document */
        TermFreqDriver termFreqDriver = new TermFreqDriver();

        String gnameWeight = args.length > 2 ? args[2] : "1.0";
        conf.setDouble("gname.weight", Double.valueOf(gnameWeight));

        String[] job2Args = args.length > 2 ? new String[3] : new String[2];
        job2Args[0] = step1_outputDir;
        job2Args[1] = step2_outputDir;
        if (args.length > 2) {
            job2Args[2] = args[2];
        }
        Job job2 = termFreqDriver.configJob(job2Args);

        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        /* step 3, calculate tf_idf */
        TF_IDF_Driver tf_idf_driver = new TF_IDF_Driver();
        String[] job3Args = new String[3];
        job3Args[0] = docCntDir;
        job3Args[1] = step2_outputDir;
        job3Args[2] = step3_outputDir;
        Job job3 = tf_idf_driver.configJob(job3Args);

        ControlledJob controlledJob3 = new ControlledJob(conf);
        controlledJob3.setJob(job3);
        controlledJob3.addDependingJob(controlledJob2);
        controlledJob3.addDependingJob(controlledPreJob);

        jobControl.addJob(controlledJob3);

        // run jobs
        runJobs(jobControl);

        return job3.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new Driver(), args));
    }
}

// End Driver.java
