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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Calculate the tf-idf for every term in each document.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class TF_IDF_Reducer extends Reducer<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private int documentNumber;

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private Map<String, String> docAndWTF = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    /**
     * Read the total document count from first step's output.
     * <p>
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./docCnt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = bufferedReader.readLine();
            this.documentNumber = Integer.parseInt(line);

            bufferedReader.close();
            fileReader.close();
        }
    }

    /**
     * @param key    term
     * @param values group_id=weighted_tf
     *               {@inheritDoc}
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        this.docAndWTF.clear();
        // total appear times of this term
        int appearInAll = 0;
        /* count the total appear times of each term
         * and store them with their weighted tf in a map  */
        for (Text value : values) {
            appearInAll++;
            // docAndFreq[0] = group_id
            // docAndFreq[1] = weighted_tf
            String[] docAndFreq = value.toString().split("=");
            this.docAndWTF.put(docAndFreq[0], docAndFreq[1]);
        }

        for (Map.Entry<String, String> entry : this.docAndWTF.entrySet()) {
            double wtf = Double.valueOf(entry.getValue());

            double idf = Math.log((double) this.documentNumber
                    / (double) (appearInAll + 1));

            this.outputKey.set(entry.getKey());
            this.outputValue.set(key.toString() + "=" + wtf * idf);
            // group_id \t term=tf_idf
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End TF_IDF_Reducer.java
