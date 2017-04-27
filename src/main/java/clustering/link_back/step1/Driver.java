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
package clustering.link_back.step1;

import clustering.Utils.MapReduceUtils;
import clustering.link_back.io.Step1KeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class to join the simhash intermediate output and the mst output.
 *
 * @author edwardlol
 *         Created by edwardlol on 2017/4/27.
 */
public class Driver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.printf("usage: %s mst_result_dir simhash_result_file output_dir\n"
                    , getClass().getSimpleName());
            System.exit(1);
        }

        Configuration conf = getConf();
        conf = MapReduceUtils.initConf(conf);

        // set the join order
        conf.set("mst", "1");
        conf.set("simhash_step1", "2");

        Job job = Job.getInstance(conf, "link back step 1 job");
        job.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(SetKeyMapper.class);
        job.setMapOutputKeyClass(Step1KeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(JoinKeyPartitioner.class);
        job.setGroupingComparatorClass(Step1GroupingComparator.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new Driver(), args));
    }
}

// End Driver.java
