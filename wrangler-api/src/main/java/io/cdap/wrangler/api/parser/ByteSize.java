package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.Locale;

public class ByteSize implements Token {

    private final long bytes;

    public ByteSize(String input){
        this.bytes = parse(input);
    }


    private long parse(String input){
        input = input.trim().toUpperCase(Locale.ENGLISH);
        if(input.endsWith("KB")){
            return (long) (Double.parseDouble(input .replace("KB","")) * 1024);
        }
        else if(input.endsWith("MB")){
            return (long) (Double.parseDouble(input.replace("MB","")) * 1024 * 1024);
        }
        else if(input.endsWith("GB")) {
            return (long) (Double.parseDouble(input.replace("GB", "")) * 1024 * 1024 * 1024);
        }
        else if(input.endsWith("B")){
                return Long.parseLong(input.replace("B",""));
        }
        else{
            // Assume it's in byte if no unit
            return Long.parseLong(input);
        }
    }

    public long getBytes() {
        return bytes;
    }

    public double getKilobytes() {
        return bytes / 1024.0;
    }

    public double getMegabytes() {
        return bytes / (1024.0 * 1024);
    }

    public double getGigabytes() {
        return bytes / (1024.0 * 1024 * 1024);
    }



    @Override
    public Object value() {
        return getBytes();
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(getBytes());
    }

    @Override
    public String toString(){
        return bytes + "bytes";
    }
}
