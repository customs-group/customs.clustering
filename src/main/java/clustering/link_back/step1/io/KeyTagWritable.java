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
package clustering.link_back.step1.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The key writable used in step 1.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-27.
 */
public class KeyTagWritable implements WritableComparable<KeyTagWritable> {
    //~ Instance fields --------------------------------------------------------

    /**
     * Join key, group id in mst.
     */
    private IntWritable joinKey = new IntWritable();

    /**
     * Secondary sort field.
     * 1 = group_id, 2 = content
     */
    private IntWritable tag = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    @Override
    public void readFields(DataInput in) throws IOException {
        this.joinKey.readFields(in);
        this.tag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.joinKey.write(out);
        this.tag.write(out);
    }

    /**
     * Main method, first sort by the joinKey, then keys with
     * the same joinKey value will have a secondary sort
     * on the value of the tag field, ensuring the order we want.
     */
    @Override
    public int compareTo(KeyTagWritable step1KeyWritable) {
        int compareValue = this.joinKey.compareTo(step1KeyWritable.getJoinKey());
        if (compareValue == 0) {
            compareValue = this.tag.compareTo(step1KeyWritable.getTag());
        }
        return compareValue;
    }

    public IntWritable getJoinKey() {
        return this.joinKey;
    }

    public IntWritable getTag() {
        return this.tag;
    }

    public void set(int key, int tag) {
        this.joinKey.set(key);
        this.tag.set(tag);
    }
}

// End KeyTagWritable.java
