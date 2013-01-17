package edu.mayo.pipes.JSON.inject;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public abstract class BaseInjector implements Injector {


	/**
	 * Injects the given key/value pair as a JSON primitive.
	 * @param object JSON object to inject the JSON primitive into.
	 * @param key
	 * @param value
	 * @param type The type of primitive
	 */
	protected void injectAsPrimitive(JsonObject object, String key, String value, JsonType type) {
		switch (type) {
		case BOOLEAN:
			object.addProperty(key, new Boolean(value));
			return;
		case NUMBER:
			object.addProperty(key, getAsNumber(value));
			break;
		case STRING:
			object.addProperty(key, value);
			return;
		}		
	}

	/**
	 * Injects the given array of values as a JSON array.
	 * @param object JSON object inject the JSON array into.
	 * @param key
	 * @param values
	 * @param type The type of primitive for each item in the array.
	 */
	protected void injectAsArray(JsonObject object, String key, String[] values, JsonType type) {
		
		JsonArray array = new JsonArray();
		
		for (String value: values) {
			switch (type) {
			case BOOLEAN:
				array.add(new JsonPrimitive(new Boolean(value)));
				break;
			case NUMBER:
				array.add(new JsonPrimitive(getAsNumber(value)));
				break;
			case STRING:
				array.add(new JsonPrimitive(value));
				break;
			}
		}
		object.add(key, array);
	}	
	
	/**
	 * Turns the given string into a number (either int or double).  Preference is 
	 * for int if possible... otherwise its a double.
	 * @param s
	 * @return
	 */
	private Number getAsNumber(String s) {
		// check if its an integer first
		try {
			return new Integer(s);
		} catch (NumberFormatException e) {}
		
		// if not an integer, must be a double
		return new Double(s);
	}

}
