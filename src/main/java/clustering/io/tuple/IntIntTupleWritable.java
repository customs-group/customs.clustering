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

import org.apache.hadoop.io.IntWritable;

/**
 * Created by edwardlol on 17-4-24.
 */
public class IntIntTupleWritable extends TupleWritable<IntWritable, IntWritable>  {
    //~ Constructors -----------------------------------------------------------

    public IntIntTupleWritable() {
        this.left = new IntWritable();
        this.right = new IntWritable();
    }

    public IntIntTupleWritable(int left, int right) {
        if (this.left == null) {
            this.left = new IntWritable();
        }
        if (this.right == null) {
            this.right = new IntWritable();
        }
        this.left.set(left);
        this.right.set(right);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public boolean equals(Object that) {
        if (!(that instanceof IntIntTupleWritable)) {
            return false;
        }
        IntIntTupleWritable guest = (IntIntTupleWritable) that;
        return (this.left.equals(guest.left) && this.right.equals(guest.right));
    }

    @Override
    public int compareTo(TupleWritable that) {
        IntIntTupleWritable guest = (IntIntTupleWritable) that;

        int result = this.left.compareTo(guest.left);
        if (result == 0) {
            return this.right.compareTo(guest.right);
        }
        return result;
    }

    @Override
    public int hashCode() {
        int hash = 17;
        hash = hash * 31 + this.left.get();
        hash = hash * 31 + this.right.get();
        return hash;
    }

    public void set(IntWritable left, IntWritable right) {
        this.left.set(left.get());
        this.right.set(right.get());
    }

    public void set(int left, int right) {
        this.left.set(left);
        this.right.set(right);
    }

    public int getLeftValue() {
        return this.left.get();
    }

    public int getRightValue() {
        return this.right.get();
    }
}

// End IntIntTupleWritable.java
