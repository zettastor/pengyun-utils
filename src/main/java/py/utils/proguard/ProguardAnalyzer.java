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
