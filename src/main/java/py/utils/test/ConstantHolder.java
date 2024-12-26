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

package py.utils.test;

/**
 * xx.
 */
public class ConstantHolder {

  public static String HELLO = "hello";

  static {
    HELLO = "123";
    HELLO = "456";
  }

  static {
    HELLO = "000000000000000000000000000000000000";
  }

  public String aa = "123";
  private String ss = "123";

  public ConstantHolder() {
    ss = "456";
    aa = "456";
  }

  @Override
  public String toString() {
    return "ConstantHolder [ss=" + ss + ", aa=" + aa + "]";
  }
}
