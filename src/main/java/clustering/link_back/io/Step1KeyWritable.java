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
package clustering.link_back.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * The key writable used in step 1.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-27.
 */
public class Step1KeyWritable extends AbstractKeyTag<IntWritable, IntWritable> implements WritableComparable<Step1KeyWritable> {
    //~ Constructors -----------------------------------------------------------

    /**
     * Join key means the group id in mst,
     * and tag is the secondary sort field,
     * 1 = group_id, 2 = content.
     */
    public Step1KeyWritable() {
        this.joinKey = new IntWritable();
        this.tag = new IntWritable();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Main method, first sort by the joinKey, then keys with
     * the same joinKey value will have a secondary sort
     * on the value of the tag field, ensuring the order we want.
     */
    @Override
    public int compareTo(Step1KeyWritable step1KeyWritable) {
        int compareValue = this.joinKey.compareTo(step1KeyWritable.getJoinKey());
        if (compareValue == 0) {
            compareValue = this.tag.compareTo(step1KeyWritable.getTag());
        }
        return compareValue;
    }

    public void set(int key, int tag) {
        this.joinKey.set(key);
        this.tag.set(tag);
    }
}

// End Step1KeyWritable.java
