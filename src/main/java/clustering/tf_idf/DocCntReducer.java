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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Calculate the document number of the corpus.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class DocCntReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputValue = new IntWritable();

    private int counter = 0;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable value : values) {
            this.counter += value.get();
        }
        this.outputValue.set(this.counter);
        context.write(NullWritable.get(), this.outputValue);
    }
}

// End DocCntReducer.java
