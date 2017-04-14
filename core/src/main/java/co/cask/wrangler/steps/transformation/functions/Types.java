package co.cask.wrangler.steps.transformation.functions;

import co.cask.wrangler.dq.TypeInference;

/**
 * Static class that is included in the Jexl expression for detecting types of data.
 */
public final class Types {
  /**
   * Checks if a value is a date or not.
   *
   * @param value representing date.
   * @return true if date, else false.
   */
  public static boolean isDate(String value) {
    return TypeInference.isDate(value);
  }

  /**
   * Checks if a value is a datetime or not.
   *
   * @param value representing date time.
   * @return true if datetime, else false.
   */
  public static boolean isTime(String value) {
    return TypeInference.isTime(value);
  }

  /**
   * Checks if a value is a number or not.
   *
   * @param value representing a number.
   * @return true if number, else false.
   */
  public static boolean isNumber(String value) {
    return TypeInference.isNumber(value);
  }


  /**
   * Checks if a value is a boolean or not.
   *
   * @param value representing a boolean.
   * @return true if boolean, else false.
   */
  public static boolean isBoolean(String value) {
    return TypeInference.isBoolean(value);
  }

  /**
   * Checks if a value is a empty or not.
   *
   * @param value representing a empty.
   * @return true if empty, else false.
   */
  public static boolean isEmpty(String value) {
    return TypeInference.isEmpty(value);
  }

  /**
   * Checks if a value is a double or not.
   *
   * @param value representing a double.
   * @return true if double, else false.
   */
  public static boolean isDouble(String value) {
    return TypeInference.isDouble(value);
  }

  /**
   * Checks if a value is a integer or not.
   *
   * @param value representing a integer.
   * @return true if integer, else false.
   */
  public static boolean isInteger(String value) {
    return TypeInference.isInteger(value);
  }
}

