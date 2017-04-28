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
package clustering.link_back;

import clustering.Utils.MapReduceUtils;
import clustering.link_back.pre.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static clustering.Utils.MapReduceUtils.initConf;

/**
 * Created by edwardlol on 17-4-28.
 */
public class WorkflowDriver extends Configured implements Tool {
    //~ Methods ----------------------------------------------------------------
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.printf("usage: %s init_input_dir simhash_intermediate_dir mst_dir output_dir\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        String pre_output = args[3] + "/pre";
        String step1_output = args[3] + "/step1";
        String step2_output = args[3] + "/final";

        Configuration conf = getConf();
        conf = initConf(conf);

        JobControl jobControl = new JobControl("link back jobs");

        Driver preDriver = new Driver();
        String[] preArgs = new String[2];
        preArgs[0] = args[0];
        preArgs[1] = pre_output;
        Job preJob = preDriver.configJob(preArgs);

        ControlledJob controlledPreJob = new ControlledJob(conf);
        controlledPreJob.setJob(preJob);
        jobControl.addJob(controlledPreJob);


        clustering.link_back.step1.Driver step1Driver = new clustering.link_back.step1.Driver();
        String[] step1Args = new String[3];
        step1Args[0] = args[2];
        step1Args[1] = args[1];
        step1Args[2] = step1_output;
        Job step1Job = step1Driver.configJob(step1Args);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(step1Job);
        jobControl.addJob(controlledJob1);

        clustering.link_back.step2.Driver driver2 = new clustering.link_back.step2.Driver();
        String[] args2 = new String[3];
        args2[0] = pre_output;
        args2[1] = step1_output;
        args2[2] = step2_output;
        Job job2 = driver2.configJob(args2);

        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledPreJob);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        MapReduceUtils.runJobs(jobControl);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new WorkflowDriver(), args));
    }
}

// End WorkflowDriver.java
