/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
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
