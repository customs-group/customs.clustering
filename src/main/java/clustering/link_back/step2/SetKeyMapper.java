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
package clustering.link_back.step2;

import clustering.link_back.io.Step2KeyWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Set the join key and join order for input data.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-27.
 */
public class SetKeyMapper extends Mapper<Text, Text, Step2KeyWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private Step2KeyWritable taggedKey = new Step2KeyWritable();

    private int joinOrder;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path filePath = fileSplit.getPath();
        this.joinOrder = filePath.toString().contains("step1") ? 1 : 2;
    }

    /**
     * @param key   entry_id@@g_no
     * @param value cluster_id or content
     *              {@inheritDoc}
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        this.taggedKey.set(key.toString(), this.joinOrder);
        // (group_id,join_order) \t cluster_id or content
        context.write(this.taggedKey, value);
    }
}

// End SetKeyMapper.java
