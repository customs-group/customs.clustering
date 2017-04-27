package clustering.link_back;

import clustering.link_back.io.AbstractKeyTag;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition the mapper output by the joinKey in step 1 and 2.
 *
 * @author edwardlol
 *         Created by edwardlol on 2017/4/27.
 */
public class JoinPartitioner extends Partitioner<AbstractKeyTag, Text> {
    //~ Methods ----------------------------------------------------------------
    @Override
    public int getPartition(AbstractKeyTag keyWritable, Text text, int numPartitions) {
        return keyWritable.getJoinKey().hashCode() % numPartitions;
    }
}

// End JoinPartitioner.java
