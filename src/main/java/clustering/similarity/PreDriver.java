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
package clustering.similarity;

import clustering.Utils.MapReduceUtils;
import clustering.io.tuple.IntIntTupleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 17-4-24.
 */
public class PreDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s inverted_index_result_dir output_dir" +
                            " [compress_or_not] [reducer number] [deci number]\n",
                    this.getClass().getSimpleName());
            System.exit(1);
        }
        Configuration conf = getConf();

        conf = MapReduceUtils.initConf(conf);
        conf.set("mapreduce.reduce.speculative", "false");

        // TODO: 17-4-24 calculate split number from reducer number
        conf.setInt("split.num", 8);

        if (args.length > 3) {
            conf.setInt("reducer.num", Integer.valueOf(args[3]));
        } else {
            conf.setInt("reducer.num", 29);
        }
        if (args.length > 4) {
            conf.setInt("deci.number", Integer.valueOf(args[4]));
        } else {
            conf.setInt("deci.number", 3);
        }

        Job job = Job.getInstance(conf, "pre job");
        job.setJarByClass(PreDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(PreMapper2.class);
        job.setMapOutputKeyClass(IntIntTupleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(PrePartitioner.class);

        job.setNumReduceTasks(conf.getInt("reducer.num", 29));
        job.setReducerClass(PreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set default compression
        if (args.length > 2 && args[2].equals("0")) {
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        } else {
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
        }

        long starttime = System.currentTimeMillis();
        boolean complete = job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("inverted similarity pre job finished in: "
                + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new PreDriver(), args));
    }
}

// End PreDriver.java
