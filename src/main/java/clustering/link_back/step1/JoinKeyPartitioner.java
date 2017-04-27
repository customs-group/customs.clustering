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
package clustering.link_back.step1;

import clustering.link_back.step1.io.KeyTagWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition the mapper output by the joinKey in {@link KeyTagWritable}.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-27.
 */
public class JoinKeyPartitioner extends Partitioner<KeyTagWritable, Text> {
    //~ Methods ----------------------------------------------------------------

    @Override
    public int getPartition(KeyTagWritable step1KeyWritable, Text text, int numPartitions) {
        return step1KeyWritable.getJoinKey().hashCode() % numPartitions;
    }
}

// End JoinKeyPartitioner.java