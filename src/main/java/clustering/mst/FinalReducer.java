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

import clustering.Utils.UnionFind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Reducer to calculate the final mst.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-27.
 */
public class FinalReducer extends Reducer<DoubleWritable, Text, IntWritable, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    private IntWritable outputValue = new IntWritable();

    private UnionFind unionFind;

    private double threshold;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.threshold = conf.getDouble("final.threshold", 0.5d);
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./docCnt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = bufferedReader.readLine();
            this.unionFind = new UnionFind(Integer.parseInt(line) + 1);

            bufferedReader.close();
            fileReader.close();
        }
    }

    /**
     * @param inputKey similarity
     * @param values   groupId1,groupId2
     *                 {@inheritDoc}
     */
    @Override
    public void reduce(DoubleWritable inputKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (inputKey.get() < this.threshold) {
            for (Text val : values) {
                String[] srcDest = val.toString().split(",");

                int src = Integer.valueOf(srcDest[0]);
                int dest = Integer.valueOf(srcDest[1]);

                this.unionFind.union(src, dest);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int[] uf = this.unionFind.getId();
        for (int i = 0; i < uf.length; i++) {
            this.outputKey.set(i);
            this.outputValue.set(this.unionFind.find(i));
            // group_id \t cluster_id
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End FinalReducer.java
