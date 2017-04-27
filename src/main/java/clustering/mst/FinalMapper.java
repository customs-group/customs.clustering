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
package clustering.mst;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class for the mst final step.
 * Convert the input weight to {@link DoubleWritable}
 * so that the reducer will receive edges sorted by their weight.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-27.
 */
public class FinalMapper extends Mapper<Text, Text, DoubleWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private DoubleWritable outputKey = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key   similarity
     * @param value doc_id1,doc_id2
     *              {@inheritDoc}
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        this.outputKey.set(Double.valueOf(key.toString()));
        // similarity \t doc_id1,doc_id2
        context.write(this.outputKey, value);
    }
}

// End FinalMapper.java
