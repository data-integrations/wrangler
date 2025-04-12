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

package io.cdap.wrangler.codec;

import com.google.gson.Gson;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.cdap.wrangler.api.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class {@link ProtobufDecoderUsingDescriptor} decodes a byte array of Protobuf
 * Records into the {@link Row} structure.
 */
public class ProtobufDecoderUsingDescriptor implements Decoder<Row> {
  private final Gson gson;
  private final Descriptors.Descriptor descriptor;


  public ProtobufDecoderUsingDescriptor(byte[] bytes, String name)
    throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException {
    this.gson = new Gson();
    DescriptorProtos.FileDescriptorSet fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(bytes);
    DescriptorProtos.FileDescriptorProto fileProto = fileDescriptorSet.getFile(0);
    Descriptors.FileDescriptor fileDescriptor =
      Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
    descriptor = fileDescriptor.findMessageTypeByName(name);
  }

  @Override
  public List<Row> decode(byte[] bytes) throws DecoderException {
    List<Row> rows = new ArrayList<>();
    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, bytes);
      Row row = new Row();
      decodeMessage(message, row, null);
      rows.add(row);
    } catch (InvalidProtocolBufferException e) {
      throw new DecoderException(e.getMessage());
    }
    return rows;
  }

  private void decodeMessage(Message message, Row row, String root) {
    for (Map.Entry<Descriptors.FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
      String name = field.getKey().getName();
      String fullName = String.format("%s", name);
      if (root != null) {
        fullName = String.format("%s_%s", root, name);
      }
      Descriptors.FieldDescriptor.Type type = field.getKey().getType();
      Object value = field.getValue();
      switch(type) {
        case MESSAGE:
          for (Message msg : (List<Message>) value) {
            decodeMessage(msg, row, fullName);
          }
          break;

        case ENUM:
          row.add(fullName, ((Descriptors.EnumValueDescriptor) value).getName());
          break;

        default:
          row.add(fullName, value);
          break;
      }
    }
  }
}
