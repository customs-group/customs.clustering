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
package clustering.mst;

import clustering.Utils.MapReduceUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * WorkflowDriver class to calculate the mst in a workflow.
 *
 * @author edwardlol
 *         Created by edwardlol on 2017/4/27.
 */
public class Driver extends Configured implements Tool {
//~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.printf("usage: %s similarity_result_dir document_count_file output_dir " +
                            "[cluster_threshold] [reduce_number] [compression]\n"
                    , getClass().getSimpleName());
            System.exit(1);
        }

        Path step1_OutputDir = new Path(args[2] + "/step1");
        Path resultDir = new Path(args[2] + "/result");

        URI docCntFile = new URI(args[1] + "/part-r-00000#docCnt");

        Configuration conf = getConf();
        conf = MapReduceUtils.initConf(conf);

        if (args.length > 3) {
            conf.setDouble("final.threshold", Double.valueOf(args[3]));
        } else {
            conf.setDouble("final.threshold", 0.2d);
        }
        if (args.length > 4) {
            conf.setInt("reduce.task.num", Integer.valueOf(args[4]));
        } else {
            conf.setInt("reduce.task.num", 5);
        }

        JobControl jobControl = new JobControl("mst jobs");

        /* step 1, split and calculate the child msts */

        Job childJob = Job.getInstance(conf, "mst child job");
        childJob.setJarByClass(Driver.class);

        childJob.addCacheFile(docCntFile);

        if (args.length > 5 && args[5].equals("0")) {
            FileInputFormat.addInputPath(childJob, new Path(args[0]));
            childJob.setInputFormatClass(KeyValueTextInputFormat.class);
        } else {
            SequenceFileInputFormat.addInputPath(childJob, new Path(args[0]));
            childJob.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        }

        FileOutputFormat.setOutputPath(childJob, step1_OutputDir);

        childJob.setMapperClass(ChildMapper.class);
        childJob.setMapOutputKeyClass(DoubleWritable.class);
        childJob.setMapOutputValueClass(Text.class);

        childJob.setPartitionerClass(ChildPartitioner.class);

        childJob.setReducerClass(ChildReducer.class);
        childJob.setNumReduceTasks(conf.getInt("reduce.task.num", 1));
        childJob.setOutputKeyClass(DoubleWritable.class);
        childJob.setOutputValueClass(Text.class);

        ControlledJob controlledChildJob = new ControlledJob(conf);
        controlledChildJob.setJob(childJob);
        jobControl.addJob(controlledChildJob);

        /* step 2, merge step 1's output and calculate final mst */

        Job finalJob = Job.getInstance(conf, "mst final job");
        finalJob.setJarByClass(FinalReducer.class);

        finalJob.addCacheFile(docCntFile);

        FileInputFormat.addInputPath(finalJob, step1_OutputDir);
        finalJob.setInputFormatClass(KeyValueTextInputFormat.class);

        finalJob.setMapperClass(FinalMapper.class);
        finalJob.setMapOutputKeyClass(DoubleWritable.class);
        finalJob.setMapOutputValueClass(Text.class);


        finalJob.setReducerClass(FinalReducer.class);
        finalJob.setOutputKeyClass(IntWritable.class);
        finalJob.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(finalJob, resultDir);

        ControlledJob finalControlledJob = new ControlledJob(conf);
        finalControlledJob.setJob(finalJob);
        finalControlledJob.addDependingJob(controlledChildJob);
        jobControl.addJob(finalControlledJob);

        // run jobs

        MapReduceUtils.runJobs(jobControl);

        return finalJob.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new Driver(), args));
    }
}

// End WorkflowDriver.java
