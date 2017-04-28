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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static clustering.Utils.MapReduceUtils.initConf;

/**
 * Pre step, count documents number in the corpus.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class DocCntDriver extends Configured implements Tool {
    //~ Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        Job job = configJob(args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job configJob(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s simhash_result_dir pre_step_output_dir\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        Configuration conf = getConf();
        conf = initConf(conf);

        Job job = Job.getInstance(conf, "tf idf pre job");
        job.setJarByClass(WorkflowDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DocCntMapper.class);
        job.setCombinerClass(DocCntReducer.class);

        job.setReducerClass(DocCntReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new DocCntDriver(), args));
    }
}

// End DocCntDriver.java
