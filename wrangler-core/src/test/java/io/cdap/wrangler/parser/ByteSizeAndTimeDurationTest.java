/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *  Copyright © 2023 Google LLC // Update copyright year/holder if needed
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for ByteSize and TimeDuration parsing and conversion.
 */
public class ByteSizeAndTimeDurationTest {

    @Test
    public void testByteSizeParsing() {
        // Verifying correct conversion from common byte size formats

        Assert.assertEquals(2048L, new ByteSize("2KB").getBytes()); // 2 KB = 2048 bytes
        Assert.assertEquals(5242880L, new ByteSize("5MB").getBytes()); // 5 MB = 5 * 1024 * 1024
        Assert.assertEquals(2147483648L, new ByteSize("2GB").getBytes()); // 2 GB = 2 * 1024^3
        Assert.assertEquals(256, new ByteSize("256B").getBytes()); // 256 bytes
        Assert.assertEquals(7340032L, new ByteSize("7MB").getBytes()); // 7 MB
        Assert.assertEquals(3145728L, new ByteSize("3MB").getBytes()); // 3 MB

        // Verifying parsing with lowercase and mixed-case units
        Assert.assertEquals(1572864L, new ByteSize("1.5mb").getBytes()); // 1.5 MB
        Assert.assertEquals(4096L, new ByteSize("4kB").getBytes()); // 4 KB
    }

    @Test
    public void testTimeDurationParsing() {
        // Verifying correct parsing of time durations into milliseconds
        double toleranceForDoubleComparison = 0.0001;

        Assert.assertEquals(10.0, new TimeDuration("10ms").
        getValue(), toleranceForDoubleComparison); // 10 milliseconds
        Assert.assertEquals(1500.0, new TimeDuration("1.5s").
        getValue(), toleranceForDoubleComparison); // 1.5 seconds
        Assert.assertEquals(7200000.0, new TimeDuration("2h").
        getValue(), toleranceForDoubleComparison); // 2 hours
        Assert.assertEquals(180000.0, new TimeDuration("3min").
        getValue(), toleranceForDoubleComparison); // 3 minutes
        Assert.assertEquals(0.5, new TimeDuration("500us").
        getValue(), toleranceForDoubleComparison); // 500
                
        Assert.assertEquals(2.5, new TimeDuration("2500000ns").
        getValue(), toleranceForDoubleComparison); // 2.5

        Assert.assertEquals(172800000.0, new TimeDuration("2d").
        getValue(), toleranceForDoubleComparison); // 2 days
    }
}
