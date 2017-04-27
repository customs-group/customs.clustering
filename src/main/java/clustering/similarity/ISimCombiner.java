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
package clustering.similarity;

import clustering.io.tuple.IntIntTupleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Add up the similarity locally.
 * The difference between this class and the reducer class is that
 * this class outputs the "similarity", and the reducer class outputs
 * the "distance", which equals 1 - similarity.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class ISimCombiner extends Reducer<IntIntTupleWritable, DoubleWritable, IntIntTupleWritable, DoubleWritable> {
    //~ Instance fields --------------------------------------------------------

    private DoubleWritable outputValue = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key    docId1,docId2
     * @param values sim
     *               {@inheritDoc}
     */
    @Override
    public void reduce(IntIntTupleWritable key, Iterable<DoubleWritable> values,
                       Context context) throws IOException, InterruptedException {
        double sim = 0.0d;
        for (DoubleWritable value : values) {
            sim += value.get();
        }
        if (sim > 0.001d) {
            this.outputValue.set(sim);
            // docId1,docId2 \t sim
            context.write(key, this.outputValue);
        }
    }
}

// End ISimCombiner.java
