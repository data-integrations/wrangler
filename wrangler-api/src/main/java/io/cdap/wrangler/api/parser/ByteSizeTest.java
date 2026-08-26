/*
 * Copyright © [2025] [Nitin]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cdap.wrangler.api.parser;

public class ByteSizeTest {
    public static void main(String[] args) {
        ByteSize size1 = new ByteSize("10KB");
        ByteSize size2 = new ByteSize("5mb");
        ByteSize size3 = new ByteSize("1.5GB");
        ByteSize size4 = new ByteSize("1TB");
        ByteSize size5 = new ByteSize("1PB");
        ByteSize size6 = new ByteSize("26B");

        System.out.println("Size1: " + size1.toBytes() + " bytes");
        System.out.println("Size2: " + size2.toBytes() + " bytes");
        System.out.println("Size3: " + size3.toBytes() + " bytes");
        System.out.println("Size4: " + size4.toBytes() + " bytes");
         System.out.println("Size5: " + size5.toBytes() + " bytes");
         System.out.println("Size6: " + size6.toBytes() + " bytes");
    }
}
