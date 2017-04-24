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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

import static clustering.Utils.MapReduceUtils.initConf;
import static clustering.Utils.MapReduceUtils.runJobs;

/**
 * Created by edwardlol on 17-4-24.
 */
public class Driver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s simhash_result_dir output_dir " +
                            "[gname_weight] [num_reduce_tasks]\n", getClass().getSimpleName());
            System.exit(1);
        }

        Path docCntDir = new Path(args[1] + "/docCount");
        Path step1_outputDir = new Path(args[1] + "/step1");
        Path step2_outputDir = new Path(args[1] + "/step2");
        Path step3_outputDir = new Path(args[1] + "/result");

        Configuration conf = getConf();
        conf = initConf(conf);

        String gnameWeight = args.length > 2 ? args[2] : "1.0";
        conf.setDouble("gname.weight", Double.valueOf(gnameWeight));

        String numReduceTasks = args.length > 3 ? args[3] : "5";
        conf.setInt("num.reduce.tasks", Integer.valueOf(numReduceTasks));

        JobControl jobControl = new JobControl("tf-idf jobs");

        /* pre step, count documents number in the corpus */

        Job preJob = Job.getInstance(conf, "tf idf pre step");
        preJob.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(preJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preJob, docCntDir);

        preJob.setMapperClass(DocCntMapper.class);
        preJob.setCombinerClass(DocCntReducer.class);

        preJob.setReducerClass(DocCntReducer.class);
        preJob.setOutputKeyClass(NullWritable.class);
        preJob.setOutputValueClass(IntWritable.class);

        ControlledJob controlledPreJob = new ControlledJob(conf);
        controlledPreJob.setJob(preJob);
        jobControl.addJob(controlledPreJob);

        /* step 1, calculate term count of each document */

        Job job1 = Job.getInstance(conf, "tf idf step1 job");
        job1.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(TermCountMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setCombinerClass(TermCountReducer.class);

        job1.setReducerClass(TermCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job1, step1_outputDir);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        /* step 2, calculate the term frequency of each document */

        Job job2 = Job.getInstance(conf, "tf idf step2 job");
        job2.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job2, step1_outputDir);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setMapperClass(TermFrequencyMapper.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(TermFrequencyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, step2_outputDir);

        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        /* step 3, calculate tf_idf */

        Job job3 = Job.getInstance(conf, "tf idf step3 job");
        job3.setJarByClass(Driver.class);

        job3.addCacheFile(new URI(docCntDir + "/part-r-00000#docCnt"));

        FileInputFormat.addInputPath(job3, step2_outputDir);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);

        job3.setMapperClass(Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(TF_IDF_Reducer.class);
        job3.setNumReduceTasks(conf.getInt("num.reduce.tasks", 5));
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job3, step3_outputDir);

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
