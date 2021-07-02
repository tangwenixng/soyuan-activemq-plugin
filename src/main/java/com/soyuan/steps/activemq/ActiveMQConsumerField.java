/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.soyuan.steps.activemq;

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

/**
 * Created by rfellows on 6/15/17.
 */
public class ActiveMQConsumerField {
  private static Class<?> PKG = ActiveMQConsumerField.class;

  private Name inputName;
  private String outputName;
  private Type outputType = Type.String;

  public ActiveMQConsumerField() {
  }

  public ActiveMQConsumerField(Name inputName, String outputName) {
    this(inputName, outputName, Type.String);
  }

  public ActiveMQConsumerField(Name inputName, String outputName, Type outputType) {
    this.inputName = inputName;
    this.outputName = outputName;
    this.outputType = outputType;
  }

  public Name getInputName() {
    return inputName;
  }

  public void setInputName(Name inputName) {
    this.inputName = inputName;
  }

  public String getOutputName() {
    return outputName;
  }

  public void setOutputName(String outputName) {
    this.outputName = outputName;
  }

  public Type getOutputType() {
    return outputType;
  }

  public void setOutputType(Type outputType) {
    this.outputType = outputType;
  }

  public enum Type {
    String("String", ValueMetaInterface.TYPE_STRING),
    Integer("Integer", ValueMetaInterface.TYPE_INTEGER),
    Binary("Binary", ValueMetaInterface.TYPE_BINARY),
    Number("Number", ValueMetaInterface.TYPE_NUMBER);

    private final String value;
    private final int valueMetaInterfaceType;

    Type(String value, int valueMetaInterfaceType) {
      this.value = value;
      this.valueMetaInterfaceType = valueMetaInterfaceType;
    }

    @Override
    public String toString() {
      return value;
    }

    boolean isEqual(String value) {
      return this.value.equals(value);
    }

    public int getValueMetaInterfaceType() {
      return valueMetaInterfaceType;
    }

    public static Type fromValueMetaInterface(ValueMetaInterface vmi) {
      if (vmi != null) {
        for (Type t : Type.values()) {
          if (vmi.getType() == t.getValueMetaInterfaceType()) {
            return t;
          }
        }
        throw new IllegalArgumentException(BaseMessages.getString(PKG,
                "ActiveMQConsumerField.Type.ERROR.NoValueMetaInterfaceMapping", vmi.getName(), vmi.getType()));
      }
      // if it's null, just default to string
      return String;
    }
  }

  public enum Name {
    MESSAGEID("messageId") {
      @Override
      public void setFieldOnMeta(ActiveMQConsumerMeta meta, ActiveMQConsumerField field) {
        meta.setMsgIdField(field);
      }

      @Override
      public ActiveMQConsumerField getFieldFromMeta(ActiveMQConsumerMeta meta) {
        return meta.getMsgIdField();
      }
    },
    MESSAGE("message") {
      @Override
      public void setFieldOnMeta(ActiveMQConsumerMeta meta, ActiveMQConsumerField field) {
        meta.setMsgField(field);
      }

      @Override
      public ActiveMQConsumerField getFieldFromMeta(ActiveMQConsumerMeta meta) {
        return meta.getMsgField();
      }
    },
    TIMESTAMP("timestamp") {
      @Override
      public void setFieldOnMeta(ActiveMQConsumerMeta meta, ActiveMQConsumerField field) {
        meta.setTimestampField(field);
      }

      @Override
      public ActiveMQConsumerField getFieldFromMeta(ActiveMQConsumerMeta meta) {
        return meta.getTimestampField();
      }
    };

    private final String name;

    Name(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }

    boolean isEqual(String name) {
      return this.name.equals(name);
    }

    public abstract void setFieldOnMeta(ActiveMQConsumerMeta meta, ActiveMQConsumerField field);

    public abstract ActiveMQConsumerField getFieldFromMeta(ActiveMQConsumerMeta meta);
  }

}
