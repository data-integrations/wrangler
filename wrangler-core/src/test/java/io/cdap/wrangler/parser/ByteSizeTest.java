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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package io.cdap.wrangler.parser;
import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

  @Test
  public void testGetBytes() {
    Assert.assertEquals(10 * 1024L, ByteSize.getBytes("10KB"));
    Assert.assertEquals((long)(1.5 * 1024 * 1024), ByteSize.getBytes("1.5MB"));
    Assert.assertEquals(3L, ByteSize.getBytes("3B"));
    Assert.assertEquals(2L * 1024 * 1024 * 1024, ByteSize.getBytes("2GB"));
    Assert.assertEquals(1L * 1024L * 1024L * 1024L * 1024L, ByteSize.getBytes("1TB"));
  }
}
