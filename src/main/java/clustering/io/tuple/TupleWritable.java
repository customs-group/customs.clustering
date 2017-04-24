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
package clustering.io.tuple;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by edwardlol on 17-4-24.
 */
abstract class TupleWritable<K extends Writable, V extends Writable> implements WritableComparable<TupleWritable> {
    //~ Instance fields --------------------------------------------------------

    K left;
    V right;

    //~ Constructors -----------------------------------------------------------

    TupleWritable() {
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null || this.getClass() != that.getClass()) {
            return false;
        }
        TupleWritable guest = (TupleWritable) that;
        return this.left.equals(guest.left) && this.right.equals(guest.right);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.left.readFields(in);
        this.right.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.left.write(out);
        this.right.write(out);
    }

    public Writable getLeft() {
        return this.left;
    }

    public Writable getRight() {
        return this.right;
    }

    @Override
    public String toString() {
        return "(" + this.left.toString() + ',' + this.right.toString() + ')';
    }

}

// End TupleWritable.java
