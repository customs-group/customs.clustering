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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Transform the input format for the Reducer.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class TermFrequencyMapper extends Mapper<Text, Text, IntWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key   term@@@group_id::position
     * @param value count
     *              {@inheritDoc}
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        // termAndDoc[0] = term
        // termAndDoc[1] = group_id::position
        String[] termAndDoc = key.toString().split("@@@");

        // idAndPosition[0] = group_id
        // idAndPosition[1] = position
        String[] idAndPosition = termAndDoc[1].split("::");

        // id
        this.outputKey.set(Integer.valueOf(idAndPosition[0]));
        // position::term=count
        this.outputValue.set(idAndPosition[1] + "::" + termAndDoc[0] + "=" + value.toString());
        // group_id \t position::term=count
        context.write(this.outputKey, this.outputValue);
    }
}

// End TermFrequencyMapper.java
