package edu.berkeley.thebes.server;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple ThebesHATServer.
 */
public class ThebesServerTest
        extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ThebesServerTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(ThebesServerTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testThebesServer() {
        assertTrue(true);
    }
}
