package clustering.tf_idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

import static clustering.Utils.MapReduceUtils.initConf;

/**
 * Calculate tf_idf.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-25.
 */
public class TF_IDF_Driver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        Job job = configJob(args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Job configJob(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.printf("usage: %s doc_cnt_dir step2_output_dir final_output_dir\n",
                    getClass().getSimpleName());
            System.exit(1);
        }
        Configuration conf = getConf();
        conf = initConf(conf);

        Job job = Job.getInstance(conf, "tf idf step3 job");
        job.setJarByClass(Driver.class);

        job.addCacheFile(new URI(args[0] + "/part-r-00000#docCnt"));

        FileInputFormat.addInputPath(job, new Path(args[1]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TF_IDF_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new TF_IDF_Driver(), args));
    }
}

// End TF_IDF_Driver.java
