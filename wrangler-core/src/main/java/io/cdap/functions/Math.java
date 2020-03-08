/*
 *  Copyright © 2020 Cask Data, Inc.
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

package io.cdap.functions;

public final class Math {

  public static double Abs(double value) {
    return java.lang.Math.abs(value);
  }

  public static double Acos(double value) {
    return java.lang.Math.acos(value);
  }

  public static double Asos(double value) {
    return java.lang.Math.asin(value);
  }

  public static double Atan(double value) {
    return java.lang.Math.atan(value);
  }

  public static double Atan2(double x, double y) {
    return java.lang.Math.atan2(x, y);
  }

  public static double Ceil(double val) {
    return java.lang.Math.ceil(val);
  }


}
