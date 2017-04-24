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
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Count the terms in each document.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class TermCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//~ Instance fields --------------------------------------------------------

    private IntWritable outputValue = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key    term@@@group_id::position
     * @param values count
     *               {@inheritDoc}
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        this.outputValue.set(sum);
        // term@@@entry_id@@g_no::position \t count
        context.write(key, this.outputValue);
    }
}

// End TermCountReducer.java
