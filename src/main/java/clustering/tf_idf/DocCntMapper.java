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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Calculate the socument number of the corpus.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class DocCntMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private final IntWritable outputValue = new IntWritable(1);

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(NullWritable.get(), this.outputValue);
    }
}

// End DocCntMapper.java
