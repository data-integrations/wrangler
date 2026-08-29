/**
 * The TokenType class provides the enumerated types for different types of
 * tokens that are supported by the grammar.
 *
 * Each of the enumerated types specified in this class also has associated
 * object representing it. e.g. {@code DIRECTIVE_NAME} is represented by the
 * object {@code DirectiveName}.
 *
 * @see Bool
 * @see BoolList
 * @see ColumnName
 * @see ColumnNameList
 * @see DirectiveName
 * @see Numeric
 * @see NumericList
 * @see Properties
 * @see Ranges
 * @see Expression
 * @see Text
 * @see TextList
 * @see ByteSize
 * @see TimeDuration
 */
@PublicEvolving
public enum TokenType implements Serializable {
// ... existing code ...
  /**
   * Represents the enumerated type for the object of type {@code String} with restrictions
   * on characters that can be present in a string.
   */
  IDENTIFIER,

  /**
   * Represents the enumerated type for the object of type {@code ByteSize} type.
   * This type is associated with byte size values with units (KB, MB, GB, TB, PB).
   * E.g.
   * <code>
   *   1KB, 2MB, 3GB, 4TB, 5PB
   * </code>
   */
  BYTE_SIZE,

  /**
   * Represents the enumerated type for the object of type {@code TimeDuration} type.
   * This type is associated with time duration values with units (ns, ms, s).
   * E.g.
   * <code>
   *   1ns, 2ms, 3s
   * </code>
   */
  TIME_DURATION
}