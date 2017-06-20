/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.codec;

import co.cask.wrangler.api.Record;
import com.google.gson.Gson;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class {@link ProtobufDecoderUsingDescriptor} decodes a byte array of Protobuf
 * Records into the {@link Record} structure.
 */
public class ProtobufDecoderUsingDescriptor implements Decoder<Record> {
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
  public List<Record> decode(byte[] bytes) throws DecoderException {
    List<Record> records = new ArrayList<>();
    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, bytes);
      Record record  = new Record();
      decodeMessage(message, record, null);
      records.add(record);
    } catch (InvalidProtocolBufferException e) {
      throw new DecoderException(e.getMessage());
    }
    return records;
  }

  private void decodeMessage(Message message, Record record, String root) {
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
            decodeMessage(msg, record, fullName);
          }
          break;

        case ENUM:
          record.add(fullName, ((Descriptors.EnumValueDescriptor) value).getName());
          break;

        default:
          record.add(fullName, value);
          break;
      }
    }
  }
}
