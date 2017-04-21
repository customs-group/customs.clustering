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
package simhash;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Calculate the simhash of every comodity.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-21.
 */
public class Step1Mapper extends Mapper<Text, Text, LongWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private LongWritable outputKey = new LongWritable();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * Calculate the SimHash signature of each commodity.
     * The output key is the signature and the output value is the
     * commodity id(entry_id + "@@" + g_no) with commodity info(g_name + "##" + g_model).
     *
     * @param key   entry_id@@g_no
     * @param value g_name##g_model
     *              {@inheritDoc}
     */
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String nameAndModel = value.toString();

        SimHash signature = SimHash.Builder.of(nameAndModel.replace("##", " ")).build();

        this.outputKey.set(signature.getHashCode());
        this.outputValue.set(key.toString() + "::" + nameAndModel);
        // simhash \t entry_id@@g_no::g_name##g_model
        context.write(this.outputKey, this.outputValue);
    }

}

// End Step1Mapper.java
