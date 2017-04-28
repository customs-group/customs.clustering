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
package clustering.link_back.step2;

import clustering.link_back.io.Step2KeyWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Join the simhash intermediate result and the mst result.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-27.
 */
public class JoinReducer extends Reducer<Step2KeyWritable, Text, Text, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private IntWritable outputValue = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key    entry_id@@g_no, join_order
     * @param values cluster_id in step1 result,
     *               or g_name \t g_model [\t else] in pre result.
     *               {@inheritDoc}
     */
    @Override
    public void reduce(Step2KeyWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // called on every group of keys
        for (Text value : values) {
            if (key.getTag().get() == 1) {
                // step result, value = cluster_id
                this.outputValue.set(Integer.valueOf(value.toString()));
            } else {
                this.outputKey.set(key.getJoinKey().toString() + "\t" + value.toString());
                // entry_id@@g_no \t g_name \t g_model [\t else], cluster_id
                context.write(this.outputKey, this.outputValue);
            }
        }
    }

}

// End JoinReducer.java
