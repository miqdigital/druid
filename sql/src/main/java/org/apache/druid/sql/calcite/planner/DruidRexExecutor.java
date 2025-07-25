/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Calcite {@code RexExecutor} that reduces Calcite expressions by evaluating them using Druid's own built-in
 * expressions. This ensures that constant reduction is done in a manner consistent with the query runtime.
 */
public class DruidRexExecutor implements RexExecutor
{
  private static final RowSignature EMPTY_ROW_SIGNATURE = RowSignature.builder().build();

  private final PlannerContext plannerContext;

  public DruidRexExecutor(final PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public void reduce(
      final RexBuilder rexBuilder,
      final List<RexNode> constExps,
      final List<RexNode> reducedValues
  )
  {
    for (RexNode constExp : constExps) {
      final DruidExpression druidExpression = Expressions.toDruidExpression(
          plannerContext,
          EMPTY_ROW_SIGNATURE,
          constExp
      );

      if (druidExpression == null) {
        reducedValues.add(constExp);
      } else {
        final SqlTypeName sqlTypeName = constExp.getType().getSqlTypeName();
        final Expr expr = Parser.parse(
            druidExpression.getExpression(),
            plannerContext.getPlannerToolbox().exprMacroTable()
        );

        final ExprEval exprResult = expr.eval(InputBindings.validateConstant(expr));

        final RexNode literal;

        if (sqlTypeName == SqlTypeName.BOOLEAN) {
          if (exprResult.value() == null) {
            literal = rexBuilder.makeNullLiteral(constExp.getType());
          } else {
            literal = rexBuilder.makeLiteral(exprResult.asBoolean(), constExp.getType(), true);
          }
        } else if (sqlTypeName == SqlTypeName.DATE) {
          if (exprResult.isNumericNull()) {
            if (constExp.getType().isNullable()) {
              literal = rexBuilder.makeNullLiteral(constExp.getType());
            } else {
              // There can be implicit casts of VARCHAR to TIMESTAMP where the VARCHAR is an invalid timestamp, but the
              // TIMESTAMP type is not nullable. In this case it's best to throw an error, since it likely means the
              // user's SQL query contains an invalid literal.
              throw InvalidSqlInput.exception("Illegal DATE constant [%s]", constExp);
            }
          } else {
            literal = rexBuilder.makeDateLiteral(
                Calcites.jodaToCalciteDateString(
                    DateTimes.utc(exprResult.asLong()),
                    plannerContext.getTimeZone()
                )
            );
          }
        } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
          if (exprResult.isNumericNull()) {
            if (constExp.getType().isNullable()) {
              literal = rexBuilder.makeNullLiteral(constExp.getType());
            } else {
              // There can be implicit casts of VARCHAR to TIMESTAMP where the VARCHAR is an invalid timestamp, but the
              // TIMESTAMP type is not nullable. In this case it's best to throw an error, since it likely means the
              // user's SQL query contains an invalid literal.
              throw InvalidSqlInput.exception("Illegal TIMESTAMP constant [%s]", constExp);
            }
          } else {
            literal = Calcites.jodaToCalciteTimestampLiteral(
                rexBuilder,
                DateTimes.utc(exprResult.asLong()),
                plannerContext.getTimeZone(),
                constExp.getType().getPrecision()
            );
          }
        } else if (SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)) {
          final BigDecimal bigDecimal;

          if (exprResult.isNumericNull()) {
            literal = rexBuilder.makeNullLiteral(constExp.getType());
          } else {
            if (exprResult.type().is(ExprType.LONG)) {
              bigDecimal = BigDecimal.valueOf(exprResult.asLong());

            } else {
              // if exprResult evaluates to Nan or infinity, this will throw a NumberFormatException.
              // If you find yourself in such a position, consider casting the literal to a BIGINT so that
              // the query can execute.
              double exprResultDouble = exprResult.asDouble();
              if (Double.isNaN(exprResultDouble) || Double.isInfinite(exprResultDouble)) {
                throw InvalidSqlInput.exception(
                        "Expression [%s] evaluates to an unsupported value [%s], expected something that"
                        + " can be a Double.  Consider casting with 'CAST(<col> AS BIGINT)'",
                        druidExpression.getExpression(),
                        exprResultDouble
                    );
              }
              bigDecimal = BigDecimal.valueOf(exprResult.asDouble());
            }
            literal = rexBuilder.makeLiteral(bigDecimal, constExp.getType(), true);
          }
        } else if (sqlTypeName == SqlTypeName.ARRAY) {
          assert exprResult.isArray();
          final Object[] array = exprResult.asArray();
          if (array == null) {
            literal = rexBuilder.makeNullLiteral(constExp.getType());
          } else if (SqlTypeName.NUMERIC_TYPES.contains(constExp.getType().getComponentType().getSqlTypeName())) {
            if (exprResult.type().getElementType().is(ExprType.LONG)) {
              List<BigDecimal> resultAsBigDecimalList = new ArrayList<>(array.length);
              for (Object val : exprResult.castTo(ExpressionType.LONG_ARRAY).asArray()) {
                final Number longVal = (Number) val;
                if (longVal == null) {
                  resultAsBigDecimalList.add(null);
                } else {
                  resultAsBigDecimalList.add(BigDecimal.valueOf(longVal.longValue()));
                }
              }
              literal = rexBuilder.makeLiteral(resultAsBigDecimalList, constExp.getType(), true);
            } else {
              List<BigDecimal> resultAsBigDecimalList = new ArrayList<>(array.length);
              for (Object val : exprResult.castTo(ExpressionType.fromColumnType(druidExpression.getDruidType())).asArray()) {
                final Number doubleVal = (Number) val;
                if (doubleVal == null) {
                  resultAsBigDecimalList.add(null);
                } else if (Double.isNaN(doubleVal.doubleValue()) || Double.isInfinite(doubleVal.doubleValue())) {
                  throw InvalidSqlInput.exception(
                          "Expression [%s] was expected to generate values that are all Doubles,"
                          + " but entry at index[%d] was not: [%s]."
                          + "  Consider Casting values to ensure a consistent type.",
                          druidExpression.getExpression(),
                          resultAsBigDecimalList.size(),
                          doubleVal
                      );
                } else {
                  resultAsBigDecimalList.add(BigDecimal.valueOf(doubleVal.doubleValue()));
                }
              }
              literal = rexBuilder.makeLiteral(resultAsBigDecimalList, constExp.getType(), true);
            }
          } else if (constExp.getType().getComponentType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            List<Boolean> resultAsBooleanList = new ArrayList<>(array.length);
            for (Object val : exprResult.castTo(ExpressionType.LONG_ARRAY).asArray()) {
              final Number longVal = (Number) val;
              if (longVal == null) {
                resultAsBooleanList.add(null);
              } else {
                resultAsBooleanList.add(Evals.asBoolean(longVal.longValue()));
              }
            }
            literal = rexBuilder.makeLiteral(resultAsBooleanList, constExp.getType(), true);
          } else {
            literal = rexBuilder.makeLiteral(Arrays.asList(array), constExp.getType(), true);
          }
        } else if (sqlTypeName == SqlTypeName.OTHER) {
          // complex constant is not reducible, so just leave it as an expression
          literal = constExp;
        } else {
          if (exprResult.isArray()) {
            // just leave array expressions on multi-value strings alone, we're going to push them down into a virtual
            // column selector anyway
            literal = constExp;
          } else {
            literal = rexBuilder.makeLiteral(exprResult.value(), constExp.getType(), true);
          }
        }

        reducedValues.add(literal);
      }
    }
  }
}
