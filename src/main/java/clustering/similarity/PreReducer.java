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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by edwardlol on 17-4-24.
 */
public class PreReducer extends Reducer<IntIntTupleWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    /**
     * The base of {@link this#decimalFormat}.
     * Also used as a threshold to prune similarity output like "0.000".
     */
    private String formatBase;

    /**
     * Format to controll number of decimals.
     */
    private DecimalFormat decimalFormat;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int deciNum = conf.getInt("deci.number", 3);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("#0.");
        for (int i = 0; i < deciNum; i++) {
            stringBuilder.append('0');
        }
        this.formatBase = stringBuilder.toString();
        this.decimalFormat = new DecimalFormat(this.formatBase);
    }

    /**
     * @param key    container_id, flag
     * @param values term_id:docId=TF-IDF,docId=TF-IDF...
     *               {@inheritDoc}
     */
    @Override
    public void reduce(IntIntTupleWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        if (key.getRightValue() == 0) {
            /* flag == 0, do self join */
            selfJoin(values, context);
        } else {
            /* flag == 1, do cross join */
            crossJoin(values, context);
        }
    }

    /**
     * @param values docId=TF-IDF,docId=TF-IDF...
     */
    private void selfJoin(Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            String[] contents = value.toString().split(",");

            if (contents.length > 1) { // should always be true, just for security
                for (int i = 0; i < contents.length; i++) {
                    for (int j = i + 1; j < contents.length; j++) {
                        String[] idAndWeighti = contents[i].split("=");
                        String[] idAndWeightj = contents[j].split("=");

                        output(idAndWeighti[0], idAndWeightj[0], idAndWeighti[1], idAndWeightj[1], context);
                    }
                }
            }
        }
    }

    /**
     * @param values docId=TF-IDF,docId=TF-IDF...#docId=TF-IDF,docId=TF-IDF...
     */
    private void crossJoin(Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            // termId, docId=TF-IDF,docId=TF-IDF...
            String[] sets = value.toString().split("#");
            String[] set1 = sets[0].split(",");
            String[] set2 = sets[1].split(",");

            for (String aPart1 : set1) {
                for (String aPart2 : set2) {
                    String[] idAndWeight1 = aPart1.split("=");
                    String[] idAndWeight2 = aPart2.split("=");

                    output(idAndWeight1[0], idAndWeight2[0], idAndWeight1[1], idAndWeight2[1], context);
                }
            }
        }
    }

    private void output(String id1, String id2,
                        String tf_idf1, String tf_idf2, Context context)
            throws IOException, InterruptedException {
        double result = Double.valueOf(tf_idf1) * Double.valueOf(tf_idf2);

        String out = this.decimalFormat.format(result);
        if (!this.formatBase.equals(out)) {
            if (Integer.valueOf(id1) > Integer.valueOf(id2)) {
                this.outputKey.set(id2 + "," + id1);
            } else {
                this.outputKey.set(id1 + "," + id2);
            }
            this.outputValue.set(out);
            // doc1,doc2 \t sim
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End PreReducer.java
