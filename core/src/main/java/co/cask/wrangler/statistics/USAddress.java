/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.statistics;

/**
 * Store structured US address
 */
public class USAddress {

  private String streetNumber = null;
  private String streetPrefix = null;
  private String streetName = null;
  private String streetSuffix = null;
  private String unitName = null;
  private String unitNumber = null;
  private String city = null;
  private String state = null;
  private String zipCode = null;

  public USAddress() {
    this.streetNumber = null;
    this.streetPrefix = null;
    this.streetName = null;
    this.streetSuffix = null;
    this.unitName = null;
    this.unitNumber = null;
    this.city = null;
    this.state = null;
    this.zipCode = null;
  }


  public USAddress(String streetNumber, String streetPrefix, String streetName, String streetSuffix,
                   String unitName, String unitNumber, String city, String state, String zipCode) {
    this.streetNumber = streetNumber;
    this.streetPrefix = streetPrefix;
    this.streetName = streetName;
    this.streetSuffix = streetSuffix;
    this.unitName = unitName;
    this.unitNumber = unitNumber;
    this.city = city;
    this.state = state;
    this.zipCode = zipCode;
  }

  public String getStreetNumber() {
    return streetNumber;
  }

  public String getStreetPrefix() {
    return streetPrefix;
  }

  public String getStreetName() {
    return streetName;
  }

  public String getStreetSuffix() {
    return streetSuffix;
  }

  public String getUnitName() {
    return unitName;
  }

  public String getUnitNumber() {
    return unitNumber;
  }

  public String getCity() {
    return city;
  }

  public String getState() {
    return state;
  }

  public String getZipCode() {
    return zipCode;
  }

  public void setStreetNumber(String streetNumber) {
    this.streetNumber = streetNumber;
  }

  public void setStreetPrefix(String streetPrefix) {
    this.streetPrefix = streetPrefix;
  }

  public void setStreetName(String streetName) {
    this.streetName = streetName;
  }

  public void setStreetSuffix(String streetSuffix) {
    this.streetSuffix = streetSuffix;
  }

  public void setUnitName(String unitName) {
    this.unitName = unitName;
  }

  public void setUnitNumber(String unitNumber) {
    this.unitNumber = unitNumber;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public void setState(String state) {
    this.state = state;
  }

  public void setZipCode(String zipCode) {
    this.zipCode = zipCode;
  }

  @Override
  public String toString() {
    return "USAddress{" + '\n' +
            "streetNumber='" + streetNumber + '\'' + '\n' +
            ", streetPrefix='" + streetPrefix + '\'' + '\n' +
            ", streetName='" + streetName + '\'' + '\n' +
            ", streetSuffix='" + streetSuffix + '\'' + '\n' +
            ", unitName='" + unitName + '\'' + '\n' +
            ", unitNumber='" + unitNumber + '\'' + '\n' +
            ", city='" + city + '\'' + '\n' +
            ", state='" + state + '\'' + '\n' +
            ", zipCode='" + zipCode + '\'' + '\n' +
            '}';
  }


  public boolean sameAs(USAddress other) {
    if (streetNumber != null && other.getStreetNumber() != null) {
      if (!streetNumber.equals(other.getStreetNumber())) {
        return false;
      }
    }
    if (streetPrefix != null && other.getStreetPrefix() != null) {
      if (!streetPrefix.equals(other.getStreetPrefix())) {
        return false;
      }
    }
    if (streetName != null && other.getStreetName() != null) {
      if (!streetName.equals(other.getStreetName())) {
        return false;
      }
    }
    if (streetSuffix != null && other.getStreetSuffix() != null) {
      if (!streetSuffix.equals(other.getStreetSuffix())) {
        return false;
      }
    }
    if (unitName != null && other.getUnitName() != null) {
      if (!unitName.equals(other.getUnitName())) {
        return false;
      }
    }
    if (unitNumber != null && other.getUnitNumber() != null) {
      if (!unitNumber.equals(other.getUnitNumber())) {
        return false;
      }
    }
    if (city != null && other.getCity() != null) {
      if (!city.equals(other.getCity())) {
        return false;
      }
    }
    if (state != null && other.getState() != null) {
      if (!state.equals(other.getState())) {
        return false;
      }
    }
    if (zipCode != null && other.getZipCode() != null) {
      if (!zipCode.equals(other.getZipCode())) {
        return false;
      }
    }
    return true;
  }
}
