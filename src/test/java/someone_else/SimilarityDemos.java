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
package someone_else;

import clustering.similarity.ISimDriver;
import clustering.similarity.PreDriver;
import org.junit.Test;

/**
 * Use cases of calculating similarities between every commodity.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-25.
 */
public class SimilarityDemos {
    //~ Methods ----------------------------------------------------------------

    // I did not merge the two step into one work flow.
    // because each step takes a lot of time.
    // Get prepared before starting this step.
    @Test
    public void preDemo() {
        PreDriver driver = new PreDriver();
        String[] args = new String[2];
        args[0] = "/base/inv_index/result";
        args[1] = "/base/sim/pre";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void resultDemo() {
        ISimDriver driver = new ISimDriver();
        String[] args = new String[2];
        args[0] = "/base/sim/pre";
        args[1] = "/base/sim/result";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// End SimilarityDemos.java
