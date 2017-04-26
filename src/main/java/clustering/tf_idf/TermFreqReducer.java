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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.edward.marog.CollectionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Calculate the term frequency for every term in each document.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class TermFreqReducer extends Reducer<IntWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private double weight;

    /**
     *
     */
    private Map<String, Double> termWeightMap = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        try {
            this.weight = conf.getDouble("gname.weight", 1.0d);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param key    group_id
     * @param values position::term=count
     *               {@inheritDoc}
     */
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int termsCntInDoc = 0;
        this.termWeightMap.clear();

        for (Text val : values) {
            // positionTermCnt[0] = position
            // positionTermCnt[1] = term=count
            String[] positionTermCnt = val.toString().split("::");
            String position = positionTermCnt[0];

            String[] termCnt = positionTermCnt[1].split("=");

            int count = Integer.valueOf(termCnt[1]);
            termsCntInDoc += count;
            // TODO: 17-4-24 is it necessary to make it enum or a class?
            double weightedCount = position.equals("title") ?
                    this.weight * count : count;

            // term : weight
            CollectionUtils.updateCountMap(this.termWeightMap, termCnt[0], weightedCount);
        }

        for (Map.Entry<String, Double> entry : this.termWeightMap.entrySet()) {
            // term
            this.outputKey.set(entry.getKey());
            // group_id=weighted_tf
            double wtf = entry.getValue() / termsCntInDoc;
            this.outputValue.set(key.toString() + "=" + wtf);
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End TermFreqReducer.java
