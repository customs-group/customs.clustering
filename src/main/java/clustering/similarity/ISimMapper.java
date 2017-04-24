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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 17-4-24.
 */
public class ISimMapper extends Mapper<Text, Text, IntIntTupleWritable, DoubleWritable> {
    //~ Instance fields --------------------------------------------------------

    private IntIntTupleWritable outputKey = new IntIntTupleWritable();

    private DoubleWritable outputValue = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key   src,dest
     * @param value sim
     *              {@inheritDoc}
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] pair = key.toString().split(",");
        this.outputKey.set(Integer.valueOf(pair[0]), Integer.valueOf(pair[1]));
        this.outputValue.set(Double.valueOf(value.toString()));
        // wrap input and write to reducer
        context.write(this.outputKey, this.outputValue);
    }
}

// End ISimMapper.java
