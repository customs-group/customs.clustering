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
package clustering.simhash;

import org.edward.marog.MathUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 17-4-21.
 */
public class SigPool extends ArrayList<Map<String, Map<Long, Integer>>> {

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    private SigPool(int listNum) {
        super();
        for (int i = 0; i < listNum; i++) {
            this.add(new HashMap<>());
        }
    }

    public static SigPool of(int listNum) {
        return new SigPool(listNum);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Check if we already have a signature similar to the input one.
     *
     * @param simHash   input simhash
     * @param threshold the threshold to determine whether two signatures are similar
     * @return The similar group_id if this.pool contains the input simhash, -1 otherwise
     */
    int hasSimilar(SimHash simHash, int threshold) {
        for (int i = 0; i < this.size(); i++) {
            String segment = simHash.getSegment(i + 1);
            Map<String, Map<Long, Integer>> seg_i = this.get(i);

            if (seg_i.containsKey(segment)) {
                Map<Long, Integer> signatures = seg_i.get(segment);
                for (Map.Entry<Long, Integer> entry : signatures.entrySet()) {
                    if (MathUtils.hammingDistance(simHash.getHashCode(), entry.getKey()) < threshold) {
                        return entry.getValue();
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Update the pool with the input simhash.
     *
     * @param simHash the input simhash
     * @param id      the group id of this simhash
     */
    void update(SimHash simHash, int id) {
        for (int i = 0; i < this.size(); i++) {

            String segment = simHash.getSegment(i + 1);
            Map<String, Map<Long, Integer>> map = this.get(i);

            Map<Long, Integer> _map = map.containsKey(segment) ? map.get(segment) : new HashMap<>();
            _map.put(simHash.getHashCode(), id);
            map.put(segment, _map);
        }
    }
}

// End SigPool.java
