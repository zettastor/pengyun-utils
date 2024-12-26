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
