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
package clustering.link_back.pre;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 17-4-28.
 */
public class AttachMapper extends Mapper<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key   entry_id
     * @param value g_no \t code_ts \t g_name \t g_name \t g_model [\t else]
     *              {@inheritDoc}
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");

        this.outputKey.set(key.toString() + "@@" + line[0]);
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < line.length; i++) {
            sb.append(line[i]).append('\t');
        }
        sb.deleteCharAt(sb.length() - 1);
        this.outputValue.set(sb.toString());
        // entry_id@@g_no \t g_name@@g_model
        context.write(this.outputKey, this.outputValue);
    }
}

// End AttachMapper.java
