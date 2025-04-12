/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

grammar Directives;

// Fragments
fragment DIGIT: [0-9];
fragment WS: [ \t\r\n]+;
fragment LETTER: [a-zA-Z];
fragment ESC: '\\' .;
fragment STRING_CHAR: ~["\\\r\n] | ESC;

// Fragments for units
fragment BYTE_UNIT: 'B' | 'KB' | 'MB' | 'GB' | 'TB' | 'PB' | 'KiB' | 'MiB' | 'GiB' | 'TiB' | 'PiB';
fragment TIME_UNIT: 'ns' | 'μs' | 'ms' | 's' | 'm' | 'h' | 'd';

// Lexer rules
STRING: '"' STRING_CHAR* '"' | '\'' STRING_CHAR* '\'';
NUMBER: DIGIT+ ('.' DIGIT+)?;
BOOLEAN: 'true' | 'false';
NULL: 'null';
IDENTIFIER: LETTER (LETTER | DIGIT | '_')*;

// Lexer rules for size and time
BYTE_SIZE: DIGIT+ ('.' DIGIT+)? WS* BYTE_UNIT;
TIME_DURATION: DIGIT+ ('.' DIGIT+)? WS* TIME_UNIT;

// Parser rules
byteSize: BYTE_SIZE;
timeDuration: TIME_DURATION;

// Add to value rule
value: 
    STRING
    | NUMBER
    | BOOLEAN
    | byteSize
    | timeDuration
    | NULL
    ; 