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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Count the terms in each document.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class TermCountMapper extends Mapper<Text, Text, Text, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private final IntWritable outputValue = new IntWritable(1);

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key   group_id
     * @param value entry_id@@g_no::g_name##[g_model]
     *              {@inheritDoc}
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("::");

        String[] nameAndModel = line[1].split("##");
        String group_id = key.toString();

        if (nameAndModel.length < 1) {
            // should not be, just for secure.
            return;
        }

        String[] nameTerms = nameAndModel[0].split(" ");
        for (String term : nameTerms) {
            this.outputKey.set(term + "@@@" + group_id + "::title");
            // term@@@group_id::title \t 1
            context.write(this.outputKey, this.outputValue);
        }
        if (nameAndModel.length == 2) {
            String[] modelTerms = nameAndModel[1].split(" ");
            for (String term : modelTerms) {
                this.outputKey.set(term + "@@@" + group_id + "::content");
                // term@@@group_id::content \t 1
                context.write(this.outputKey, this.outputValue);
            }
        }
    }
}

// End TermCountMapper.java
