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
package clustering.inverted_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The reducer class for calculating inverted index
 * to combine all inverted indexes together.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
//~ Static fields/initializers ---------------------------------------------

    /**
     * used to initialize term ids
     */
    private static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private DecimalFormat decimalFormat;

    private boolean pruning;

    private double pruningThreshold;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int deci_num = conf.getInt("deci.number", 3);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("0.");
        for (int i = 0; i < deci_num; i++) {
            stringBuilder.append('0');
        }
        this.decimalFormat = new DecimalFormat(stringBuilder.toString());
        this.pruning = conf.getBoolean("pruning", false);
        this.pruningThreshold = conf.getDouble("pruning.threshold", 0.1d);
    }

    /**
     * @param key    term
     * @param values group_id=tf-idf
     *               {@inheritDoc}
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int id = count.incrementAndGet();

        StringBuilder sb = new StringBuilder();

        /* append the index of this term */
        for (Text value : values) {
            String[] idAndTFIDF = value.toString().split("=");
            double tf_idf = Double.valueOf(idAndTFIDF[1]);
            /* filter the small tf_idfs
            which contributes little to the final similarity */
            if (!(this.pruning && tf_idf < this.pruningThreshold)) {
                sb.append(idAndTFIDF[0]).append('=').append(this.decimalFormat.format(tf_idf)).append(',');
            }
        }
        try {
            sb.deleteCharAt(sb.length() - 1);
        } catch (RuntimeException e) {
            System.out.println(sb.toString());
        }

        this.outputKey.set(id + ":" + key.toString());
        this.outputValue.set(sb.toString());
        // termId:term \t group_id=tf-idf,...
        context.write(this.outputKey, this.outputValue);
    }
}

// End InvertedIndexReducer.java
