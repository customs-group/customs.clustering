package clustering.tf_idf;

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

import static clustering.Utils.MapReduceUtils.initConf;

/**
 * Calculate the term frequency of each document.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-25.
 */
public class TermFreqDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        Job job = configJob(args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job configJob(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s step_1_output_dir step_2_output_dir " +
                    "[gname_weight]\n", getClass().getSimpleName());
            System.exit(1);
        }
        Configuration conf = getConf();
        conf = initConf(conf);

        String gnameWeight = args.length > 2 ? args[2] : "1.0";
        conf.setDouble("gname.weight", Double.valueOf(gnameWeight));

        Job job = Job.getInstance(conf, "tf idf step2 job");
        job.setJarByClass(WorkflowDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(TermFreqMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TermFreqReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new TermFreqDriver(), args));
    }
}

// End TermFreqDriver.java
