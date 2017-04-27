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

import clustering.link_back.io.Step1KeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Grouping comparator to group mapper keys.
 * Mapper keys with the same joinKey will be grouped together in the reducer,
 * ignore the tag value.
 *
 * @author edwardlol
 *         Created by edwardlol on 2017/4/27.
 */
public class Step1GroupingComparator extends WritableComparator {
    //~ Constructors -----------------------------------------------------------

    public Step1GroupingComparator() {
        super(Step1KeyWritable.class, true);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Step1KeyWritable taggedKey1 = (Step1KeyWritable) a;
        Step1KeyWritable taggedKey2 = (Step1KeyWritable) b;
        return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
    }
}

// End Step1GroupingComparator.java
