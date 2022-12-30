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

package py.expression.test;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import py.test.TestBase;

/**
 * xx.
 */
public class RuleTester extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(RuleTester.class);
  private static final JexlEngine jexl = new JexlEngine();

  /**
   * xx.
   */
  @SuppressWarnings("unchecked")
  private static void processExpression(MyObject containerObject, String expressionString) {
    try {
      JexlContext jc = new MapContext();
      // "myObject" is the alias to be used for expression evaluation
      jc.set("myObject", containerObject);
      jc.set("Ns", containerObject.getNumber1());
      jc.set("Nd", containerObject.getNumber2());
      jc.set("Nf", containerObject.getNumber3());
      Expression expression = jexl.createExpression(expressionString);
      logger.debug("Evaluating: " + expression);
      logger.debug(" \\--------> " + expression.evaluate(jc));
    } catch (Exception e) {
      logger.error("Caught an exception", e);
    }
  }
  
  /**
   * xx.
   */
  @SuppressWarnings("unchecked")
  private static void processExpression1(MyObject containerObject, String expressionString) {
    try {
      JexlContext jc = new MapContext();
      // "myObject" is the alias to be used for expression evaluation
      jc.set("myObject", containerObject);
      jc.set("N1", containerObject.getNumber1());
      jc.set("N2", containerObject.getNumber2());
      jc.set("N3", containerObject.getNumber3());
      Expression expression = jexl.createExpression(expressionString);
      logger.debug("Evaluating: " + expression);
      logger.debug(" \\--------> " + expression.evaluate(jc));
    } catch (Exception e) {
      logger.error("Caught an exception", e);
    }
  }

  @Test
  public void test() {
    MyObject myObject = new MyObject();

    String expr;
    expr = "(myObject.number1 + myObject.number2) / myObject.number3";
    processExpression(myObject, expr);
    expr = "(N1 + N2) / N3";
    processExpression(myObject, expr);
    expr = "myObject.itsTrue";
    processExpression(myObject, expr);
    expr = "myObject.itsFalse";
    processExpression(myObject, expr);

    expr = "myObject.getThePresident().equals('Barack Obama')";
    processExpression(myObject, expr);
    expr = "myObject.thePresident.equals(\"George Bush\")";
    processExpression(myObject, expr);

    expr = "((N1 + N2) > N3 && myObject.itsFalse==false) || N2 > N3";
    processExpression(myObject, expr);

  }

  @Test
  public void test1() throws Exception {
    String expr = "((N1 + N2) > N3 && myObject.itsFalse==false) || N2 > N3";

    // Expression e = ExpressionFactory.createExpression(expr);
    // JexlContext jc = JexlHelper.createContext();

    MyObject myObject = new MyObject();
    processExpression(myObject, expr);
  }

  @Test
  public void test_long() throws Exception {
    try {
      String expr = "N1>1996383492428932748";
      MyObject myObject = new MyObject();
      processExpression(myObject, expr);
    } catch (Exception e) {
      Assert.fail();
    }
  }

  /**
   * xx.
   */
  public class MyObject {

    private String thePresident = "Barack Obama";
    private Boolean itsTrue = true;
    private Boolean itsFalse = false;
    private Long number1 = 15L;
    private Integer number2 = 13;
    private Integer number3 = 2;

    public String getThePresident() {
      return thePresident;
    }

    public void setThePresident(String thePresident) {
      this.thePresident = thePresident;
    }

    public Boolean getItsTrue() {
      return itsTrue;
    }

    public void setItsTrue(Boolean itsTrue) {
      this.itsTrue = itsTrue;
    }

    public Boolean getItsFalse() {
      return itsFalse;
    }

    public void setItsFalse(Boolean itsFalse) {
      this.itsFalse = itsFalse;
    }

    public Long getNumber1() {
      return number1;
    }

    public void setNumber1(Long number1) {
      this.number1 = number1;
    }

    public Integer getNumber2() {
      return number2;
    }

    public void setNumber2(Integer number2) {
      this.number2 = number2;
    }

    public Integer getNumber3() {
      return number3;
    }

    public void setNumber3(Integer number3) {
      this.number3 = number3;
    }

  }
}
