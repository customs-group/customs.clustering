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
package clustering.simhash;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Take one comodity out of every group.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-21.
 */
public class Step2Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    //~ Methods ----------------------------------------------------------------

    /**
     * @param key    group_id
     * @param values entry_id@@g_no::g_name##g_model
     *               {@inheritDoc}
     */
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // the first one in this group as a representative
        Text representative = values.iterator().next();
        // group_id, entry_id@@g_no::g_name##g_model
        context.write(key, representative);
    }
}

// End Step2Reducer.java
