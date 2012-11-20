package edu.mayo.pipes;

import edu.mayo.pipes.DrainPipeTest;
import edu.mayo.pipes.SplitPipeTest;
import edu.mayo.pipes.util.OracleConnectionTest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses( { 
        DrainPipeTest.class,
        OracleConnectionTest.class,
	SplitPipeTest.class
 })
public class AllPipesTest {

	// NOTE: Add any new test case classes to the @Suite.SuiteClasses annotation above
}

