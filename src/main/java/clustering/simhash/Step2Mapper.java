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
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Transform input key to IntWritable.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-25.
 */
public class Step2Mapper extends Mapper<Text, Text, IntWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private final IntWritable outputKey = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key   id
     * @param value entry_id@@g_no::g_name##g_model
     *              {@inheritDoc}
     */
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        this.outputKey.set(Integer.valueOf(key.toString()));
        context.write(this.outputKey, value);
    }
}

// End Step2Mapper.java
