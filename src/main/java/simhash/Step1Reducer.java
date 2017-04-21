package simhash;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Group the comodities by their simhash.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-21.
 */
public class Step1Reducer extends Reducer<LongWritable, Text, IntWritable, Text> {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Atomic integer used to initialize document ids.
     */
    protected static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    /**
     * A list of map (simhash_signature, document_id).
     */
    private List<Map<String, Map<Long, Integer>>> pool = new ArrayList<>();

    /**
     * The threshold of signature similarity.
     * Signatures with similarity below this value will be grouped
     * together and been considered as duplicated.
     */
    private int threshold;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.threshold = context.getConfiguration().getInt("simhash.threshold", 3);

        for (int i = 0; i < this.threshold + 1; i++) {
            this.pool.add(new HashMap<>());
        }
    }

    /**
     * @param key    simhash
     * @param values entry_id@@g_no::g_name##g_model
     *               {@inheritDoc}
     */
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            String[] keyAndContent = value.toString().split("::");

//            SimHash thisHash = new SimHash(keyAndContent[1], key.get());
//
//            int id = contains(thisHash);
//            if (id == -1) { // does not contain
//                id = count.incrementAndGet();
//                add(thisHash, id);
//            }
//            this.outputKey.set(id);
            // group_id \t entry_id@@g_no::g_name##g_model
            context.write(this.outputKey, value);
        }
    }

    /**
     * add the input simhash to this.pool
     *
     * @param simHash the input simhash
     * @param id      the group id of this simhash
     */
    private void add(SimHash simHash, int id) {
        String[] segments = simHash.getSegments(this.threshold + 1);

        for (int i = 0; i < this.threshold + 1; i++) {
            Map<String, Map<Long, Integer>> map = this.pool.get(i);

            Map<Long, Integer> _map = map.containsKey(segments[i]) ? map.get(segments[i]) : new HashMap<>();
            _map.put(simHash.getHashCode(), id);
            map.put(segments[i], _map);
        }
    }

    /**
     * check if this.pool contains the input simhash
     *
     * @param simHash input simhash
     * @return id if this.pool contains the input simhash, -1 otherwise
     */
    private int contains(SimHash simHash) {
        String[] segments = simHash.getSegments(this.threshold + 1);

        for (int i = 0; i < this.threshold + 1; i++) {
            Map<String, Map<Long, Integer>> map = this.pool.get(i);
            if (map.containsKey(segments[i])) {
                Map<Long, Integer> signatures = map.get(segments[i]);
                for (Map.Entry<Long, Integer> entry : signatures.entrySet()) {
//                    if (simHash.hammingDistance(entry.getKey()) < this.threshold) {
//                        return entry.getValue();
//                    }
                }
            }
        }
        return -1;
    }

}

// End Step1Reducer.java
