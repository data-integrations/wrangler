/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonElement;

public class ByteSize implements Token {
	private final long bytes;
	private final String unit;
	private final double value;

	public ByteSize(String value) {
		String numericPart = value.replaceAll("[^0-9.]", "");
		this.value = Double.parseDouble(numericPart);
		this.unit = value.replaceAll("[0-9.]", "").trim().toUpperCase();

		switch (this.unit) {
			case "B":
				this.bytes = Math.round(this.value);
				break;
			case "KB":
				this.bytes = Math.round(this.value * 1024);
				break;
			case "MB":
				this.bytes = Math.round(this.value * 1024 * 1024);
				break;
			case "GB":
				this.bytes = Math.round(this.value * 1024 * 1024 * 1024);
				break;
			case "TB":
				this.bytes = Math.round(this.value * 1024 * 1024 * 1024 * 1024L);
				break;
			default:
				throw new IllegalArgumentException("Unsupported byte unit: " + this.unit);
		}
	}

	public long getBytes() {
		return bytes;
	}

	public double getKilobytes() {
		return bytes / 1024.0;
	}

	public double getMegabytes() {
		return bytes / (1024.0 * 1024.0);
	}

	public double getGigabytes() {
		return bytes / (1024.0 * 1024.0 * 1024.0);
	}

	public double getTerabytes() {
		return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
	}

	public double getValue() {
		return value;
	}

	public String getUnit() {
		return unit;
	}

	@Override
	public JsonElement toJson() {
		JsonObject json = new JsonObject();
		json.add("value", new JsonPrimitive(value));
		json.add("unit", new JsonPrimitive(unit));
		json.add("bytes", new JsonPrimitive(bytes));
		return json;
	}

	@Override
	public Object value() {
		return bytes;
	}

	@Override
	public TokenType type() {
		return TokenType.BYTE_SIZE;
	}
}