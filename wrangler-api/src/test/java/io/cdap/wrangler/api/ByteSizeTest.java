// /*
//  * Copyright © 2017-2019 Cask Data, Inc.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  * use this file except in compliance with the License. You may obtain a copy of
//  * the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  * License for the specific language governing permissions and limitations under
//  * the License.
//  */

// import org.junit.Test;

// import io.cdap.wrangler.api.parser.ByteSize;

// import static org.junit.Assert.assertEquals;

// public class ByteSizeTest {

//     @Test
//     public void testValidByteSizeKB() {
//         ByteSize byteSize = new ByteSize("10KB");
//         assertEquals(10240, byteSize.getBytes());
//     }

//     @Test
//     public void testValidByteSizeMB() {
//         ByteSize byteSize = new ByteSize("1MB");
//         assertEquals(1048576, byteSize.getBytes());
//     }

//     @Test
//     public void testValidByteSizeGB() {
//         ByteSize byteSize = new ByteSize("2GB");
//         assertEquals(2147483648L, byteSize.getBytes());
//     }

//     @Test
//     public void testValidByteSizeWithoutUnit() {
//         ByteSize byteSize = new ByteSize("500");
//         assertEquals(500L, byteSize.getBytes());
//     }

//     @Test
//     public void testInvalidByteSize() {
//         assertThrows(IllegalArgumentException.class, () -> {
//             new ByteSize("invalidSize");
//         });
//     }

//     @Test
//     public void testInvalidByteSizeUnit() {
//         assertThrows(IllegalArgumentException.class, () -> {
//             new ByteSize("10ZZ");
//         });
//     }
// }
