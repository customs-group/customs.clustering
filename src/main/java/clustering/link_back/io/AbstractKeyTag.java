package clustering.link_back.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An abstract base for key writables.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-27.
 */
public abstract class AbstractKeyTag<K extends Writable, T extends Writable>
        implements Writable {
    //~ Instance fields --------------------------------------------------------

    protected K joinKey;

    protected T tag;

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

    public K getJoinKey() {
        return this.joinKey;
    }

    public T getTag() {
        return this.tag;
    }

}

// End AbstractKeyTag.java
