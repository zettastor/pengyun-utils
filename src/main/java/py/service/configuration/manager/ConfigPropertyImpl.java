/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.service.configuration.manager;

import java.io.Serializable;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * xx.
 */
@XmlRootElement(name = "property")
@XmlType(propOrder = {"name", "value", "range", "subProperties"})
@XmlAccessorType(XmlAccessType.NONE)
public class ConfigPropertyImpl implements Serializable, ConfigProperty {

  private static final long serialVersionUID = -4947585650384940228L;

  @XmlAttribute(name = "name")
  private String name;

  @XmlAttribute(name = "value")
  private String value;

  @XmlAttribute(name = "range")
  private String range;

  // @XmlElementWrapper(name = "subProperties")
  @XmlElement(name = "sub_property")
  private List<ConfigSubProperty> subProperties;

  public ConfigPropertyImpl() {
  }

  public ConfigPropertyImpl(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getRange() {
    return range;
  }

  public void setRange(String range) {
    this.range = range;
  }

  public List<ConfigSubProperty> getSubProperties() {
    return subProperties;
  }

  public void setSubProperties(List<ConfigSubProperty> subProperties) {
    this.subProperties = subProperties;
  }

  @Override
  public String toString() {
    return "ConfigProperty [name=" + name + ", value=" + value + ", range=" + range
        + ", subProperties="
        + subProperties + "]";
  }

}
