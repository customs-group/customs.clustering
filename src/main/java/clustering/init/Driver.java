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
package clustering.init;

import clustering.Utils.MapReduceUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class for initialization jobs.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-21.
 */
public class Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s input_dir output_dir [column_splitter] [dict_path]\n",
                    this.getClass().getSimpleName());
            System.exit(1);
        }
        Configuration conf = getConf();

        conf = MapReduceUtils.initConf(conf);

        if (args.length > 2) {
            conf.set("column.splitter", args[2]);
        } else {
            conf.set("column.splitter", ",");
        }

        if (args.length > 3) {
            conf.set("dict.path", args[3]);
        } else {
            conf.set("dict.path", "./dicts");
        }

        Job job = Job.getInstance(conf, "Initialization job");
        job.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(WordSepMapper.class);

        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long starttime = System.currentTimeMillis();
        boolean complete = job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("Initialization job finished in: " + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
    }

    //~ Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new Driver(), args));
    }
}

// End Driver.java
