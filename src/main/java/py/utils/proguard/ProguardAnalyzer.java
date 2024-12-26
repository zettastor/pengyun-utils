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

package py.utils.proguard;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.beans.PropertyDescriptor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

/**
 * xx.
 */
public class ProguardAnalyzer {

  private static final PrintStream out = System.out;

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    if (3 != args.length) {
      out.println("Err: invalid argument!");
      System.exit(1);
    }

    File classDir = new File(args[0]);
    File outputFile = new File(args[1]);
    String jar = args[2];

    Set<String> properties = new HashSet<String>();
    Set<String> methods = new HashSet<String>();

    File[] classes = classDir.listFiles();
    for (File classFile : classes) {
      try {
        String className = classFile.getName();
        if (className.endsWith(".class")) {
          className = className.substring(0, className.indexOf(".class"));
          Class clazz = Class.forName(className);
          BeanInfo info = Introspector.getBeanInfo(clazz);

          for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
            if (pd.getName().length() >= 5) {
              properties.add(pd.getName());
            }
          }

          for (MethodDescriptor md : info.getMethodDescriptors()) {
            if (md.getName().length() >= 10) {
              methods.add(md.getName());
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true));
    bw.write("######### " + jar + " ##########");
    bw.newLine();

    bw.write("**** Properties ****");
    bw.newLine();
    for (String property : properties) {
      bw.write(property);
      bw.newLine();
    }

    bw.write("**** Methods ****");
    bw.newLine();
    for (String method : methods) {
      bw.write(method);
      bw.newLine();
    }

    bw.close();
  }
}
