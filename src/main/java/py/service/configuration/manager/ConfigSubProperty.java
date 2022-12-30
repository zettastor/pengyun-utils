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
