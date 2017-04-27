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
package clustering.mst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Mapper to calculate a local mst.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-26.
 */
public class ChildMapper extends Mapper<Text, Text, DoubleWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private DoubleWritable outputKey = new DoubleWritable();

    private Text outputValue = new Text();

    private int docsInSeg;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int docCnt = 0;
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./docCnt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            docCnt = Integer.valueOf(bufferedReader.readLine());

            bufferedReader.close();
            fileReader.close();
        }
        Configuration conf = context.getConfiguration();

        int reduceTaskNum = conf.getInt("reduce.task.num", 3);
        this.docsInSeg = docCnt / reduceTaskNum;
        if (docCnt % reduceTaskNum != 0) {
            this.docsInSeg++;
        }
    }

    /**
     * @param key   group_id1,group_id2
     * @param value similarity
     *              {@inheritDoc}
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String idPair = key.toString();
        String[] ids = idPair.split(",");

        int id1 = Integer.valueOf(ids[0]);
        int id2 = Integer.valueOf(ids[1]);

        // get the weight
        double weight = Double.valueOf(value.toString());
        this.outputKey.set(weight);

        int container = belongsTo(id1, id2);
        this.outputValue.set(idPair + ":" + container);

        // weight \t src,dest:containder_id
        context.write(this.outputKey, this.outputValue);
    }

    /**
     * Check which container this edge belongs to.
     * id1 is always < id2
     *
     * @return the container id
     */
    private int belongsTo(int id1, int id2) {
        int container1 = (id1 - 1) / this.docsInSeg;
        int container2 = (id2 - 1) / this.docsInSeg;

        if (container1 == container2) {
            return container1;
        }

        if (id2 % this.docsInSeg >= (this.docsInSeg / 2)) {
            return container2;
        } else {
            return container1;
        }
    }
}

// End ChildMapper.java
