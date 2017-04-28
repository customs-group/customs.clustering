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

import clustering.mst.Driver;
import org.junit.Test;

/**
 * Demos for calculating minimal spanning tree in pararrel.
 *
 * @author edwardlol
 *         Created by edwardlol on 2017/4/27.
 */
public class MSTDemos {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void mstWorkflowDemo() {
        Driver driver = new Driver();
        String[] args = new String[3];
        args[0] = "/base/sim/isim";
        args[1] = "/base/tf_idf/docCnt";
        args[2] = "/base/mst";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void setThreshold() {
        Driver driver = new Driver();
        String[] args = new String[4];
        args[0] = "/base/sim/isim";
        args[1] = "/base/tf_idf/docCnt";
        args[2] = "/base/mst";
        args[3] = "0.18";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// End MSTDemos.java
