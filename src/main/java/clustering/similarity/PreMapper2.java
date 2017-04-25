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
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Devide and distribute the indexes.
 * There are two strategies,
 * if the index is short, just send it to a container and the container will do a self join;
 * if the index is long, I first devide it into {@link this#splitNum} splits.
 * The output key consists of two parts: container id and splited flag.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-24.
 */
public class PreMapper2 extends Mapper<Text, Text, IntIntTupleWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private IntIntTupleWritable outputKey = new IntIntTupleWritable();

    private Text outputValue = new Text();

    /**
     * container index for short inverted_indexes
     */
    private int smallIndex = 0;

    /**
     * container index for long inverted_indexes
     */
    private int bigIndex = 0;

    /**
     * the number of splits to divide a long index into
     */
    private int splitNum;

    /**
     * threshold of index length to determine
     * whether to divide an index or not
     */
    private int lengthThreshold;

    private StringBuilder sb1 = new StringBuilder();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.splitNum = conf.getInt("split.num", 6);
        this.lengthThreshold = conf.getInt("length.threshold", 1000);
    }

    /**
     * @param key     id:term
     * @param value   group_id=tf-idf,...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] docs = value.toString().split(",");

        if (docs.length > this.lengthThreshold) {
            /* set the length of each container */
            int docsInSeg = docs.length / this.splitNum;
            if (docs.length % this.splitNum != 0) {
                docsInSeg++;
            }

            for (int i = 0; i < this.splitNum - 1; i++) {
                this.sb1.setLength(0);

                /*  */
                for (int a = 0; a < docsInSeg; a++) {
                    this.sb1.append(docs[i * docsInSeg + a]).append(',');
                }
                this.sb1.deleteCharAt(this.sb1.length() - 1);

                this.outputKey.set(this.bigIndex++, 0);
                this.outputValue.set(this.sb1.toString());
                // container_id,flag \t term_id:group_id=tf-idf,...
                context.write(this.outputKey, this.outputValue);

                for (int j = i + 1; j < this.splitNum; j++) {
                    /* continue from sb1 */
                    StringBuilder sb2 = new StringBuilder(this.sb1).append('#');
                    for (int b = 0; b < docsInSeg; b++) {
                        int index = j * docsInSeg + b;
                        if (index >= docs.length) {
                            break;
                        }
                        sb2.append(docs[index]).append(',');
                    }
                    sb2.deleteCharAt(sb2.length() - 1);

                    this.outputKey.set(this.bigIndex++, 1);
                    this.outputValue.set(sb2.toString());
                    // container_id,flag \t term_id:group_id=tf-idf,...
                    context.write(this.outputKey, this.outputValue);
                }
            }

            this.sb1.setLength(0);
            for (int a = (this.splitNum - 1) * docsInSeg; a < docs.length; a++) {
                this.sb1.append(docs[a]).append(',');
            }
            this.sb1.deleteCharAt(this.sb1.length() - 1);

            this.outputKey.set(this.bigIndex++, 0);
            this.outputValue.set(this.sb1.toString());
            context.write(this.outputKey, this.outputValue);
        } else if (docs.length > 1) {
            this.outputKey.set(smallIndex++, 0);
            this.outputValue.set(value.toString());
            // container_id,flag \t term_id:group_id=tf-idf,...
            context.write(this.outputKey, this.outputValue);
        }
    }

}

// End PreMapper.java
