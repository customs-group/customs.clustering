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
package clustering.tf_idf.deprecated;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Transform the input format for the Reducer.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class TF_IDF_Mapper extends Mapper<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key   term@@@group_id
     * @param value weighted term frequency
     *              {@inheritDoc}
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] termAndDoc = key.toString().split("@@@");

        this.outputKey.set(termAndDoc[0]);
        this.outputValue.set(termAndDoc[1] + "=" + value.toString());
        // term \t group_id=weighted_tf
        context.write(this.outputKey, this.outputValue);
    }

}

// End TF_IDF_Mapper.java
