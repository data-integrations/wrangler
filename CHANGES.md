Here's how you can proceed with the Wrangler enhancement: 
   

   Step 1: Fork the Repository 
   Step 2: Modify the Grammar.......//  Modify the parser rules to accept these tokens.
   Step 3: Update the API (wrangler-api)......//Update the Token Types to support these new types.



  Changes that i have done :
Define Byte as a BYTE_SIZE,and Time as TIMEDURATION classes.

THEN I FIX IT IN THE WRABGLER CORE FOLDER \\Directives.g4

Extend the base Token class correctly (from io.cdap.wrangler.api.parser.Token).
 // Add lexer rules for BYTE_SIZE and TIME_DURATION
BYTE : [0-9]+ ('KB'|'MB'|'GB'|'B');
TIME_DURATION : [0-9]+ ('ms'|'s'|'m'|'h'|'d');

// parser rules
byteSizeArg : BYTE;
timeDurationArg : TIME;

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class Byte implements Token {
    private final long bytes;

    public Byte(String value) {
        this.bytes = parseByteSize(value.trim().toUpperCase());
    }

    private long parseByteSize(String value) {
        if (value.endsWith("KB")) {
            return (long)(Double.parseDouble(value.replace("KB", "")) * 1024);
        } else if (value.endsWith("MB")) {
            return (long)(Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
        } else if (value.endsWith("GB")) {
            return (long)(Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
        } else if (value.endsWith("B")) {
            return Long.parseLong(value.replace("B", ""));
        } else {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
        }
    }

    public long getBytes() {
        return bytes;
    }

    @Override
    public Object value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(bytes);
    }
}

........

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class Time implements Token {
    private final long nanoseconds;

    public Time (String value) {
        this.nanoseconds = parseTimeDuration(value.trim().toLowerCase());
    }

    private long parseTimeDuration(String value) {
        if (value.endsWith("ns")) {
            return Long.parseLong(value.replace("ns", ""));
        } else if (value.endsWith("ms")) {
            return (long)(Double.parseDouble(value.replace("ms", "")) * 1_000_000);
        } else if (value.endsWith("s")) {
            return (long)(Double.parseDouble(value.replace("s", "")) * 1_000_000_000);
        } else if (value.endsWith("m")) {
            return (long)(Double.parseDouble(value.replace("m", "")) * 60 * 1_000_000_000L);
        } else if (value.endsWith("h")) {
            return (long)(Double.parseDouble(value.replace("h", "")) * 3600 * 1_000_000_000L);
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }
    }

    public long getNanoseconds() {
        return nanoseconds;
    }

    @Override
    public Object value() {
        return nanoseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(nanoseconds);
    }
}

........


package io.cdap.wrangler.api.parser;

public enum Tokenn {
    BYTE,
    TIME;
}

//BYTE_SIZE
//TIME_DURATION

.......
  IDENTIFIER, BYTE, TIME


  ....
  
