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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Reducer to calculate the local mst.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-26.
 */
public class ChildReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
//~ Instance fields --------------------------------------------------------

    private Text outputValue = new Text();

    private UnionFind unionFind;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
     * @param values   doc_id1,doc_id2:container
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(DoubleWritable inputKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            String[] srcDestPair = value.toString().split(":");

            String[] srcDest = srcDestPair[0].split(",");

            int src = Integer.valueOf(srcDest[0]);
            int dest = Integer.valueOf(srcDest[1]);

            if (this.unionFind.union(src, dest)) {
                this.outputValue.set(srcDestPair[0]);
                context.write(inputKey, this.outputValue);
            }
        }
    }
}

// End ChildReducer.java
