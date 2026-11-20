package io.cdap.wrangler.parser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DirectivesLexerTest {

    @Test
    public void testByteSizeTokens() {
        String input = "10KB 1.5MB 2GB 0.5TB 100B";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        List<? extends Token> tokens = lexer.getAllTokens();

        assertEquals(5, tokens.size());
        assertEquals("10KB", tokens.get(0).getText());
        assertEquals("1.5MB", tokens.get(1).getText());
        assertEquals("2GB", tokens.get(2).getText());
        assertEquals("0.5TB", tokens.get(3).getText());
        assertEquals("100B", tokens.get(4).getText());
    }

    @Test
    public void testTimeDurationTokens() {
        String input = "10ms 1.5s 2m 0.5h 100ns";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        List<? extends Token> tokens = lexer.getAllTokens();

        assertEquals(5, tokens.size());
        assertEquals("10ms", tokens.get(0).getText());
        assertEquals("1.5s", tokens.get(1).getText());
        assertEquals("2m", tokens.get(2).getText());
        assertEquals("0.5h", tokens.get(3).getText());
        assertEquals("100ns", tokens.get(4).getText());
    }

    @Test
    public void testMixedTokens() {
        String input = "10KB 1.5s 2GB 0.5h";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        List<? extends Token> tokens = lexer.getAllTokens();

        assertEquals(4, tokens.size());
        assertEquals("10KB", tokens.get(0).getText());
        assertEquals("1.5s", tokens.get(1).getText());
        assertEquals("2GB", tokens.get(2).getText());
        assertEquals("0.5h", tokens.get(3).getText());
    }
}