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

import clustering.simhash.Driver;
import org.junit.Test;

/**
 * Use cases of pre-clustering(SimHash) step.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-25.
 */
public class SimHashDemos {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void defaultThreshold() {
        Driver driver = new Driver();
        String[] args = new String[2];
        args[0] = "/base/out/0901";
        args[1] = "/base/simhash";

        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void customizedThreshold() {
        Driver driver = new Driver();
        String[] args = new String[3];
        args[0] = "/base/out/0901";
        args[1] = "/base/simhash2";
        args[2] = "2";

        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// End SimHashDemos.java
