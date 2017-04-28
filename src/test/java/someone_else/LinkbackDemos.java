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

import clustering.link_back.WorkflowDriver;
import clustering.link_back.step1.Driver;
import org.junit.Test;

/**
 * Created by edwardlol on 17-4-28.
 */
public class LinkbackDemos {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void workflow() {
        WorkflowDriver driver = new WorkflowDriver();
        String[] args = new String[4];
        args[0] = "/base/in/0901";
        args[1] = "/base/simhash/step1";
        args[2] = "/base/mst/result";
        args[3] = "/base/linkback";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // following 3 tests are splitting the workflow into 3 steps
    // in case you need to debug the outputs
    @Test
    public void pre() {
        clustering.link_back.pre.Driver driver = new clustering.link_back.pre.Driver();
        String[] args = new String[2];
        args[0] = "/base/in/0901";
        args[1] = "/base/result/pre";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step1() {
        Driver driver = new Driver();
        String[] args = new String[3];
        args[0] = "/base/mst/result";
        args[1] = "/base/simhash/step1";
        args[2] = "/base/result/step1";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step2() {
        clustering.link_back.step2.Driver driver = new clustering.link_back.step2.Driver();
        String[] args = new String[3];
        args[0] = "/base/result/pre";
        args[1] = "/base/result/step1";
        args[2] = "/base/result/final";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

// End LinkbackDemos.java
