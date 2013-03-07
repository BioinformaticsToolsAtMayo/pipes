package edu.mayo.pipes.util.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseCommandProcess {
	// environment for the child process
	// each string has the format "name=value"
	protected String[] mEnvironment;
	
	protected String[] mCmdArray;
		
	/**
	 * Constructor
	 * 
	 * @param customEnv
	 * @param useParentEnv
	 */
	public BaseCommandProcess(String command, String[] commandArgs, Map<String, String> customEnv, boolean useParentEnv) {
		setupEnvironment(customEnv, useParentEnv);
		mCmdArray = toCommandArray(command, commandArgs);
	}
	
	/**
	 * Initializes the environment that will be used by the child process.
	 */
	private void setupEnvironment(Map<String, String> customEnv, boolean useParentEnv) {
		
		Map<String, String> env = new HashMap<String, String>();
		
		// JVM environment inherited from parent shell process
		if (useParentEnv) {
			env.putAll(System.getenv());
		}
		
		// custom environment is done 2nd
		// will potentially overwrite variables with the same name 
		env.putAll(customEnv);
		
		mEnvironment = translateMapToStrings(env);
	}
	
	/**
	 * Translates a map of key/value pairs into an array of strings with format "key=value".
	 * 
	 * @param map The key/value map to be translated.
	 * @return
	 */
	private String[] translateMapToStrings(Map<String, String> map) {
		
		List<String> list = new ArrayList<String>();
		
		for (String key: map.keySet()) {
			String val = map.get(key);			
			list.add(key + "=" + val);
		}
		
		return list.toArray(new String[0]);
	}
	
	/**
	 * Builds a string array "cmdarray" used by the java.lang.Runtime.exec() method.
	 * 
	 * 
	 * @param command
	 * @param arguments
	 * @return array containing the command to call and its arguments.
	 */
	private String[] toCommandArray(String command, String[] arguments) {
		// translate command and command args into a string array
		List<String> cmdList = new ArrayList<String>();
		cmdList.add(command);
		for (String arg : arguments) {
			cmdList.add(arg);
		}
		return cmdList.toArray(new String[0]);		
	}	
}
