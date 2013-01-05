package edu.mayo.pipes.util;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class DelimVocabTest {
	
	@Test
	public void wordsAsDelim() {
		assertEquals(",", DelimVocab.toRegEX("comma"));
		assertEquals("\\|", DelimVocab.toRegEX("pipe"));
		assertEquals(";", DelimVocab.toRegEX("semicolon"));
		assertEquals(":", DelimVocab.toRegEX("colon"));
		assertEquals("=", DelimVocab.toRegEX("equal"));
		assertEquals("\t", DelimVocab.toRegEX("tab"));
		assertEquals("\\.", DelimVocab.toRegEX("period"));
		assertEquals(" +", DelimVocab.toRegEX("space"));
		
		// Word that is not supported should just go straight thru
		assertEquals("percent", DelimVocab.toRegEX("percent"));
	}
	
	@Test 
	public void charAsDelim() {
		assertEquals("\\|", DelimVocab.toRegEX("|"));
		assertEquals("\t", DelimVocab.toRegEX("\t"));
		assertEquals(",", DelimVocab.toRegEX(","));
		assertEquals(";", DelimVocab.toRegEX(";"));
		assertEquals(":", DelimVocab.toRegEX(":"));
		assertEquals("\\.", DelimVocab.toRegEX("."));
		assertEquals("=", DelimVocab.toRegEX("="));
		assertEquals(" +", DelimVocab.toRegEX(" "));
		assertEquals("%", DelimVocab.toRegEX("%"));
		assertEquals("x", DelimVocab.toRegEX("x"));
		assertEquals("~", DelimVocab.toRegEX("~"));
	}
	
}
