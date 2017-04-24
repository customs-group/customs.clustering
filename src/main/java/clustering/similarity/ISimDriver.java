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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 17-4-24.
 */
public class ISimDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s simpre_dir output_dir " +
                            "[compression_or_not] [reduce_task_number]\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        Configuration conf = getConf();
        conf = MapReduceUtils.initConf(conf);

        Job job = Job.getInstance(conf, "isim job");
        job.setJarByClass(ISimDriver.class);

        if (args.length > 2 && args[2].equals("0")) {
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        } else {
            job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
            SequenceFileInputFormat.addInputPath(job, new Path(args[0]));

            conf.setBoolean("mapreduce.map.output.compress", true);
            conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
        }

        if (args.length > 3) {
            conf.setInt("reduce.num", Integer.valueOf(args[3]));
        } else {
            conf.setInt("reduce.num", 1);
        }

        job.setMapperClass(ISimMapper.class);
        job.setMapOutputKeyClass(IntIntTupleWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(ISimCombiner.class);
        job.setPartitionerClass(HashPartitioner.class);

        job.setNumReduceTasks(conf.getInt("reduce.num", 1));

        job.setReducerClass(ISimReducer.class);
        job.setOutputKeyClass(IntIntTupleWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        long starttime = System.currentTimeMillis();
        boolean complete = job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("inverted similarity job finished in: "
                + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new ISimDriver(), args));
    }
}

// End ISimDriver.java
