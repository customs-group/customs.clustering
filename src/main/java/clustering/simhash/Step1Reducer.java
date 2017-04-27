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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Group the comodities by their simhash.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-21.
 */
public class Step1Reducer extends Reducer<LongWritable, Text, IntWritable, Text> {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Atomic integer used to initialize document ids.
     */
    private static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    /**
     * The signature pool used to fast find similarity signatures.
     * It is a list of map (signature_segment, submap),
     * and each of the submap's entry consists of (full_signature, document_id).
     */
    private SigPool _pool;

    /**
     * The threshold of signature similarity.
     * Signatures with similarity below this value will be grouped
     * together and been considered as duplicated.
     */
    private int threshold;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.threshold = context.getConfiguration().getInt("simhash.threshold", 3);

        this._pool = SigPool.of(this.threshold + 1);
    }

    /**
     * @param key    simhash
     * @param values entry_id@@g_no::g_name##g_model
     *               {@inheritDoc}
     */
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            String[] docIdAndCommoInfo = value.toString().split("::");

            SimHash thisHash = SimHash.Builder.of(docIdAndCommoInfo[1]).build(key.get());

            int id = this._pool.hasSimilar(thisHash, this.threshold);
            if (id == -1) { // does not contain
                id = count.incrementAndGet();
                this._pool.update(thisHash, id);
            }
            this.outputKey.set(id);
            // group_id \t entry_id@@g_no::g_name##g_model
            context.write(this.outputKey, value);
        }
    }
}

// End Step1Reducer.java
