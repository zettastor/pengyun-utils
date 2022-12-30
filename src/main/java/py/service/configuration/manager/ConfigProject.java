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
@XmlRootElement(name = "project")
@XmlType(propOrder = {"name", "files"})
@XmlAccessorType(XmlAccessType.NONE)
public class ConfigProject implements Serializable {

  private static final long serialVersionUID = 2151825563174160845L;

  @XmlAttribute(name = "name")
  private String name;

  //@XmlElementWrapper(name = "files")
  @XmlElement(name = "file")
  private List<ConfigFile> files;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<ConfigFile> getFiles() {
    return files;
  }

  public void setFiles(List<ConfigFile> files) {
    this.files = files;
  }

  @Override
  public String toString() {
    return "Project [name=" + name + ", files=" + files + "]";
  }

}
