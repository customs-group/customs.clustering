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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Normalize the tf-idf result so that
 * each vector has a norm of 1.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class NormalizerReducer extends Reducer<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    /**
     * (term, tf_idf)
     */
    private Map<String, Double> tf_idfs = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key    group_id
     * @param values term=tf_idf
     *               {@inheritDoc}
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        this.tf_idfs.clear();
        for (Text value : values) {
            String[] termAndTF_IDF = value.toString().split("=");
            this.tf_idfs.put(termAndTF_IDF[0], Double.valueOf(termAndTF_IDF[1]));
        }
        double sum = 0.0d;
        for (double tf_idf : this.tf_idfs.values()) {
            sum += Math.pow(tf_idf, 2);
        }
        final double sq_sum = Math.sqrt(sum);

        this.tf_idfs.replaceAll((k, v) -> v / sq_sum);

        for (Map.Entry<String, Double> entry : this.tf_idfs.entrySet()) {
            this.outputKey.set(entry.getKey());
            this.outputValue.set(key.toString() + "=" + entry.getValue().toString());
            // term \t group_id=normalized_tf_idf
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End NormalizerReducer.java
