package clustering.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by edwardlol on 17-4-24.
 */
public class IndexSegWritable implements Writable {
    //~ Instance fields --------------------------------------------------------

    List<Text> left;

    List<Text> right;

    boolean hasRight;

    //~ Methods ----------------------------------------------------------------

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hasRight = dataInput.readBoolean();
        int leftSize = dataInput.readInt();
        if (this.hasRight) {
            int rightSize = dataInput.readInt();
            this.right = new ArrayList<>(rightSize);
        }

        this.left = new ArrayList<>(leftSize);
        for (int i = 0; i < leftSize; i++) {
            Text item = new Text();
            item.readFields(dataInput);
            this.left.add(item);
        }

        if (this.hasRight) {
            for (int i = 0; i < this.right.size(); i++) {
                Text item = new Text();
                item.readFields(dataInput);
                this.right.add(item);
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(this.hasRight);
        dataOutput.writeInt(this.left.size());
        if (this.hasRight) {
            dataOutput.writeInt(this.right.size());
        }

        for (Text item : this.left) {
            item.write(dataOutput);
        }
        if (this.hasRight) {
            for (Text item : this.right) {
                item.write(dataOutput);
            }
        }
    }

    public boolean hasRight() {
        return this.hasRight;
    }

    public void setHasRight(boolean hasRight) {
        this.hasRight = hasRight;
    }

    public void addLeft(String item) {
        // TODO: 17-4-24 should improve new Text()
        this.left.add(new Text(item));
    }
}

// End IndexSegWritable.java
