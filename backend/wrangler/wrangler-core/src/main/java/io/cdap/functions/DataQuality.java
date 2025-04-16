/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import io.cdap.wrangler.api.Row;
import org.apache.commons.validator.routines.CreditCardValidator;
import org.apache.commons.validator.routines.DateValidator;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.ISBNValidator;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.commons.validator.routines.UrlValidator;

/**
 * Data Quality Checks consolidated.
 */
public class DataQuality extends Types {
  private DataQuality() {
  }

  /**
   * Given a row, finds the length of the row.
   *
   * @param row length needs to be determined.
   * @return length of the row.
   */
  public static int columns(Row row) {
    return row.width();
  }

  /**
   * Finds if the row has a column.
   *
   * @param row in which a column needs to be checked.
   * @param column name of the column to be checked.
   * @return true if column is not null and exists, false otherwise.
   */
  public static boolean hascolumn(Row row, String column) {
    if (column == null) {
      return false;
    }
    return row.find(column) != -1 ? true : false;
  }

  /**
   * Checks if the value is within the range.
   *
   * @param value to be checked if it's in the range.
   * @param lower end of the defined range.
   * @param upper end of the defined range inclusive.
   * @return true if in range, false otherwise.
   */
  public static boolean inrange(double value, double lower, double upper) {
    Range<Double> range = Range.range(lower, BoundType.CLOSED, upper, BoundType.CLOSED);
    if (range.contains(value)) {
      return true;
    }
    return false;
  }

  /**
   * Returns the length of the string.
   *
   * @param str for which we need to determine the length.
   * @return length of string if not null, 0 otherwise.
   */
  public static int strlen(String str) {
    if (str != null) {
      return str.length();
    }
    return 0;
  }

  /**
   * Checks if the object is null.
   *
   * @param object to be checked for null.
   * @return true if
   */
  public static boolean isnull(Object object) {
    return object == null ? true : false;
  }

  /**
   * Checks if the string is empty or not.
   *
   * @param str to be checked for empty.
   * @return true if not null and empty, else false.
   */
  public static boolean isempty(String str) {
    if (str != null && str.isEmpty()) {
      return true;
    }
    return false;
  }

  /**
   * Validate using the default <code>Locale</code>.
   *
   * @param date The value validation is being performed on.
   * @return <code>true</code> if the value is valid.
   */
  public static boolean isDate(String date) {
    return DateValidator.getInstance().isValid(date);
  }

  /**
   * Validate using the specified <i>pattern</i>.
   *
   * @param date The value validation is being performed on.
   * @param pattern The pattern used to validate the value against.
   * @return <code>true</code> if the value is valid.
   */
  public static boolean isDate(String date, String pattern) {
    return DateValidator.getInstance().isValid(date, pattern);
  }

  /**
   * Validates if string <code>ip</code> is a valid IP address or
   * not. Could be IPv4 or IPv6.
   *
   * @param ip to be validated.
   * @return true if valid IPv4 or IPv6.
   */
  public static boolean isIP(String ip) {
    return InetAddressValidator.getInstance().isValid(ip);
  }

  /**
   * Validates if string <code>ip</code> is a valid IPv4 address or
   * not.
   *
   * @param ip to be validated.
   * @return true if valid IPv4.
   */
  public static boolean isIPv4(String ip) {
    return InetAddressValidator.getInstance().isValidInet4Address(ip);
  }

  /**
   * Validates if string <code>ip</code> is a valid IPv6 address or
   * not.
   *
   * @param ip to be validated.
   * @return true if valid IPv6.
   */
  public static boolean isIPv6(String ip) {
    return InetAddressValidator.getInstance().isValidInet6Address(ip);
  }

  /**
   * Validates if string <code>ip</code> is a valid email address or
   * not.
   *
   * @param email to be validated.
   * @return true if valid email.
   */
  public static boolean isEmail(String email) {
    return EmailValidator.getInstance().isValid(email);
  }

  /**
   * Validates if string <code>ip</code> is a valid url address or
   * not.
   *
   * @param url to be validated.
   * @return true if valid url.
   */
  public static boolean isUrl(String url) {
    return UrlValidator.getInstance().isValid(url);
  }

  /**
   * Validates if string <code>ip</code> is a valid url domain or
   * not.
   *
   * @param domain to be validated.
   * @return true if valid url.
   */
  public static boolean isDomainName(String domain) {
    return DomainValidator.getInstance().isValid(domain);
  }

  /**
   * Validates if string <code>ip</code> is a valid top-level domain or
   * not.
   *
   * @param domain to be validated.
   * @return true if valid top-level domain.
   */
  public static boolean isDomainTld(String domain) {
    return DomainValidator.getInstance().isValidTld(domain);
  }

  /**
   * Validates if string <code>ip</code> is a valid generic top-level domain or
   * not.
   *
   * @param domain to be validated.
   * @return true if valid generic top-level domain.
   */
  public static boolean isGenericTld(String domain) {
    return DomainValidator.getInstance().isValidGenericTld(domain);
  }

  /**
   * Validates if string <code>ip</code> is a valid country top-level domain or
   * not.
   *
   * @param domain to be validated.
   * @return true if valid country top-level domain.
   */
  public static boolean isCountryTld(String domain) {
    return DomainValidator.getInstance().isValidCountryCodeTld(domain);
  }

  /**
   * Validates if string <code>ip</code> is a valid ISBN-10 or ISBN-13 or
   * not.
   *
   * @param isbn to be validated.
   * @return true if valid ISBN-10 or ISBN-13.
   */
  public static boolean isISBN(String isbn) {
    return ISBNValidator.getInstance().isValid(isbn);
  }

  /**
   * Validates if string <code>ip</code> is a valid ISBN-10 or
   * not.
   *
   * @param isbn to be validated.
   * @return true if valid ISBN-10.
   */
  public static boolean isISBN10(String isbn) {
    return ISBNValidator.getInstance().isValidISBN10(isbn);
  }

  /**
   * Validates if string <code>ip</code> is a valid ISBN-13 or
   * not.
   *
   * @param isbn to be validated.
   * @return true if valid ISBN-13.
   */
  public static boolean isISBN13(String isbn) {
    return ISBNValidator.getInstance().isValidISBN13(isbn);
  }

  /**
   * Validates if string <code>ip</code> is a valid credit card number or
   * not.
   *
   * @param cc to be validated.
   * @return true if valid credit card number.
   */
  public static boolean isCreditCard(String cc) {
    return CreditCardValidator.genericCreditCardValidator().isValid(cc);
  }

  /**
   * Validates if string <code>ip</code> is a valid amex credit card number or
   * not.
   *
   * @param cc to be validated.
   * @return true if valid amex credit card number.
   */
  public static boolean isAmex(String cc) {
    return CreditCardValidator.AMEX_VALIDATOR.isValid(cc);
  }

  /**
   * Validates if string <code>ip</code> is a valid visa credit card number or
   * not.
   *
   * @param cc to be validated.
   * @return true if valid visa credit card number.
   */
  public static boolean isVisa(String cc) {
    return CreditCardValidator.VISA_VALIDATOR.isValid(cc);
  }

  /**
   * Validates if string <code>ip</code> is a valid master credit card number or
   * not.
   *
   * @param cc to be validated.
   * @return true if valid master credit card number.
   */
  public static boolean isMaster(String cc) {
    return CreditCardValidator.MASTERCARD_VALIDATOR.isValid(cc);
  }

  /**
   * Validates if string <code>ip</code> is a valid diner credit card number or
   * not.
   *
   * @param cc to be validated.
   * @return true if valid diner credit card number.
   */
  public static boolean isDiner(String cc) {
    return CreditCardValidator.DINERS_VALIDATOR.isValid(cc);
  }

  /**
   * Validates if string <code>ip</code> is a valid discover credit card number or
   * not.
   *
   * @param cc to be validated.
   * @return true if valid discover credit card number.
   */
  public static boolean isDiscover(String cc) {
    return CreditCardValidator.DISCOVER_VALIDATOR.isValid(cc);
  }

  /**
   * Validates if string <code>ip</code> is a valid VPay credit card number or
   * not.
   *
   * @param cc to be validated.
   * @return true if valid VPay credit card number.
   */
  public static boolean isVPay(String cc) {
    return CreditCardValidator.VPAY_VALIDATOR.isValid(cc);
  }
}
