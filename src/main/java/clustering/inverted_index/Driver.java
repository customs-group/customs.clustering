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
package clustering.inverted_index;

import clustering.Utils.MapReduceUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generate an inverted index from TF-IDF output.
 * The inverted index is formatted like: term_id:term \t [document_id=tf-idf,...].
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class Driver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s tf_idf_result_dir output_dir" +
                            "[decimal_number] [pruning_threshold]\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        Path normDir = new Path(args[1] + "/normed");
        Path resultDir = new Path(args[1] + "/result");

        Configuration conf = getConf();
        conf = MapReduceUtils.initConf(conf);

        if (args.length > 2) {
            conf.setInt("deci.number", Integer.valueOf(args[2]));
        } else {
            conf.setInt("deci.number", 3);
        }

        if (args.length > 3) {
            conf.setBoolean("pruning", true);
            conf.setDouble("pruning.threshold", Double.valueOf(args[3]));
        } else {
            conf.setBoolean("pruning", false);
        }

        JobControl jobControl = new JobControl("inverted-index jobs");

        /* step 1, normalize the vector lenth of each document */

        Job job1 = Job.getInstance(conf, "tf idf normalizer job");
        job1.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(Mapper.class);

        job1.setReducerClass(NormalizerReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, normDir);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        /* step 2, calculate inverted index */

        Job job2 = Job.getInstance(conf, "inverted index job");
        job2.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job2, normDir);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setMapperClass(Mapper.class);

        job2.setReducerClass(InvertedIndexReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, resultDir);

        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        MapReduceUtils.runJobs(jobControl);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new Driver(), args));
    }

}

// End Driver.java
