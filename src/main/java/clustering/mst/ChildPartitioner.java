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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition the edges.
 * Partition by their end points so that the sub-graphs are
 * both connected and evenly distributed.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-26.
 */
public class ChildPartitioner extends Partitioner<DoubleWritable, Text> {
    //~ Methods ----------------------------------------------------------------

    /**
     * @param key   weight
     * @param value (group_id1,group_id2):container_id
     *              {@inheritDoc}
     */
    @Override
    public int getPartition(DoubleWritable key, Text value, int numPartitions) {
        if (numPartitions == 0) {
            return 0;
        }
        String[] contents = value.toString().split(":");

        return Integer.valueOf(contents[1]) % numPartitions;
    }
}

// End ChildPartitioner.java
