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
@XmlRootElement(name = "file")
@XmlType(propOrder = {"name", "properties"})
@XmlAccessorType(XmlAccessType.NONE)
public class ConfigFile implements Serializable {

  private static final long serialVersionUID = -6116893736576870694L;

  @XmlAttribute(name = "name")
  private String name;

  //@XmlElementWrapper(name = "properties")
  @XmlElement(name = "property")
  private List<ConfigPropertyImpl> properties;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<ConfigPropertyImpl> getProperties() {
    return properties;
  }

  public void setProperties(List<ConfigPropertyImpl> properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    return "File [name=" + name + ", properties=" + properties + "]";
  }

}
