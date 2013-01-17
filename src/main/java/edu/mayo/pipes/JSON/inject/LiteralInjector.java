package edu.mayo.pipes.JSON.inject;

import com.google.gson.JsonObject;

import edu.mayo.pipes.history.History;

/**
 * Injects a literal name/value pair "as is" into the JSON object.
 */
public class LiteralInjector extends BaseInjector implements Injector {

	private String mKey;
	private String mValue;
	private JsonType   mType;
	
	/**
	 * Constructor
	 * 
	 * @param key JSON primitive name
	 * @param value JSON primitive value
	 * @param type JSON primitive type
	 */
	public LiteralInjector(String key, String value, JsonType type) {
		mKey = key;
		mValue = value;
		mType = type;
	}
	
	@Override
	public void inject(JsonObject object, History history) {
		super.injectAsPrimitive(object, mKey, mValue, mType);
	}
	
}
