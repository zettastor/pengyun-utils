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
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * xx.
 */
@XmlRootElement(name = "value")
@XmlType(propOrder = {"index", "value", "range"})
@XmlAccessorType(XmlAccessType.NONE)
public class ConfigSubProperty implements Serializable, ConfigProperty {

  private static final long serialVersionUID = -9125737301789570554L;

  @XmlAttribute(name = "index")
  private String index = "0";

  @XmlAttribute(name = "value")
  private String value;

  @XmlAttribute(name = "range")
  private String range;

  public String getValue() {
    return value;
  }

  public void setValue(String subValue) {
    this.value = subValue;
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public String getRange() {
    return range;
  }

  public void setRange(String range) {
    this.range = range;
  }

  @Override
  public String toString() {
    return "SubProperty [index=" + index + ", value=" + value + ", range=" + range + "]";
  }

}
