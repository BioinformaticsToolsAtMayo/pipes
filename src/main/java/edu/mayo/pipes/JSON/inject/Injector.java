package edu.mayo.pipes.JSON.inject;

import com.google.gson.JsonObject;

import edu.mayo.pipes.history.History;

/**
 * Injects data into an existing JSON object.
 */
public interface Injector {
	
	/**
	 * Inject data into the given JSON object.
	 * 
	 * @param object Existing object to be modified.
	 * @param history History for the current row
	 */
	public void inject(JsonObject object, History history);
}
