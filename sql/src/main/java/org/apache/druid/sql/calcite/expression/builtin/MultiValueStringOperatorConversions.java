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

package org.apache.druid.sql.calcite.expression.builtin;

import com.google.common.collect.Sets;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ListFilteredVirtualColumn;
import org.apache.druid.segment.virtual.PrefixFilteredVirtualColumn;
import org.apache.druid.segment.virtual.RegexFilteredVirtualColumn;
import org.apache.druid.sql.calcite.expression.AliasedOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.DruidLiteral;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;

/**
 * Array functions which return an array, but are used in a multi-valued string dimension context instead will output
 * {@link SqlTypeName#VARCHAR} instead of {@link SqlTypeName#ARRAY}. On the backend, these functions are identical,
 * so these classes only override the signature information.
 */
public class MultiValueStringOperatorConversions
{
  public static final SqlOperatorConversion CONTAINS = new Contains();
  public static final SqlOperatorConversion OVERLAP = new Overlap();


  public static class Append extends ArrayAppendOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_APPEND")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_APPEND(array, expr)'",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.family(SqlTypeFamily.STRING)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    public Append()
    {
      super(SQL_FUNCTION, ArrayAppendOperatorConversion.FUNCTION_NAME);
    }
  }

  public static class Prepend extends ArrayPrependOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_PREPEND")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_PREPEND(expr, array)'",
                OperandTypes.family(SqlTypeFamily.STRING),
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                )
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    public Prepend()
    {
      super(SQL_FUNCTION, ArrayPrependOperatorConversion.FUNCTION_NAME);
    }
  }

  public static class Concat extends ArrayConcatOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_CONCAT")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_CONCAT(array, array)'",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                )
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    public Concat()
    {
      super(SQL_FUNCTION, ArrayConcatOperatorConversion.FUNCTION_NAME);
    }
  }

  /**
   * Extends {@link ArrayContainsOperatorConversion} to get the filter conversion behavior.
   * Private: use singleton {@link #CONTAINS}.
   */
  private static class Contains extends ArrayContainsOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_CONTAINS")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_CONTAINS(array, array)'",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING),
                    OperandTypes.family(SqlTypeFamily.NUMERIC)
                )
            )
        )
        .returnTypeInference(ReturnTypes.BOOLEAN_NULLABLE)
        .build();

    public Contains()
    {
      super(SQL_FUNCTION, "mv_contains");
    }
  }

  public static class Offset extends ArrayOffsetOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_OFFSET")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_OFFSET(array, expr)'",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.family(SqlTypeFamily.NUMERIC)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    public Offset()
    {
      super(SQL_FUNCTION, ArrayOffsetOperatorConversion.FUNCTION_NAME);
    }
  }

  public static class Ordinal extends ArrayOrdinalOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_ORDINAL")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_ORDINAL(array, expr)'",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.family(SqlTypeFamily.NUMERIC)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    public Ordinal()
    {
      super(SQL_FUNCTION, ArrayOrdinalOperatorConversion.FUNCTION_NAME);
    }
  }

  public static class Slice extends ArraySliceOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_SLICE")
        .operandTypeChecker(
            OperandTypes.or(
                OperandTypes.sequence(
                    "'MV_SLICE(expr, start)'",
                    OperandTypes.or(
                        OperandTypes.family(SqlTypeFamily.ARRAY),
                        OperandTypes.family(SqlTypeFamily.STRING)
                    ),
                    OperandTypes.family(SqlTypeFamily.NUMERIC)
                ),
                OperandTypes.sequence(
                    "'MV_SLICE(expr, start, end)'",
                    OperandTypes.or(
                        OperandTypes.family(SqlTypeFamily.ARRAY),
                        OperandTypes.family(SqlTypeFamily.STRING)
                    ),
                    OperandTypes.family(SqlTypeFamily.NUMERIC),
                    OperandTypes.family(SqlTypeFamily.NUMERIC)
                )
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    public Slice()
    {
      super(SQL_FUNCTION, ArraySliceOperatorConversion.FUNCTION_NAME);
    }
  }

  public static class StringToMultiString extends StringToArrayOperatorConversion
  {
    public static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("STRING_TO_MV")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'STRING_TO_MV(string, expr)'",
                OperandTypes.family(SqlTypeFamily.STRING),
                OperandTypes.family(SqlTypeFamily.STRING)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    public StringToMultiString()
    {
      super(SQL_FUNCTION, StringToArrayOperatorConversion.FUNCTION_NAME);
    }
  }

  public static class MultiStringToString extends AliasedOperatorConversion
  {
    public MultiStringToString()
    {
      super(new ArrayToStringOperatorConversion(), "MV_TO_STRING");
    }
  }

  public static class Length extends AliasedOperatorConversion
  {
    public Length()
    {
      super(new ArrayLengthOperatorConversion(), "MV_LENGTH");
    }
  }

  public static class OffsetOf extends AliasedOperatorConversion
  {
    public OffsetOf()
    {
      super(new ArrayOffsetOfOperatorConversion(), "MV_OFFSET_OF");
    }
  }

  public static class OrdinalOf extends AliasedOperatorConversion
  {
    public OrdinalOf()
    {
      super(new ArrayOrdinalOfOperatorConversion(), "MV_ORDINAL_OF");
    }
  }

  /**
   * Extends {@link ArrayOverlapOperatorConversion} to get the filter conversion behavior.
   * Private: use singleton {@link #OVERLAP}.
   */
  private static class Overlap extends ArrayOverlapOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_OVERLAP")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_OVERLAP(array, array)'",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING),
                    OperandTypes.family(SqlTypeFamily.NUMERIC)
                )
            )
        )
        .returnTypeInference(ReturnTypes.BOOLEAN_NULLABLE)
        .build();

    public Overlap()
    {
      super(SQL_FUNCTION, "mv_overlap");
    }
  }

  private abstract static class ListFilter implements SqlOperatorConversion
  {
    abstract boolean isAllowList();

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      final RexCall call = (RexCall) rexNode;

      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands()
      );

      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      final DruidExpression.ExpressionGenerator builder = (args) -> {
        final StringBuilder expressionBuilder;
        if (isAllowList()) {
          expressionBuilder = new StringBuilder("filter((x) -> array_contains(");
        } else {
          expressionBuilder = new StringBuilder("filter((x) -> !array_contains(");
        }

        expressionBuilder.append(args.get(1).getExpression())
                         .append(", x), ")
                         .append(args.get(0).getExpression())
                         .append(")");
        return expressionBuilder.toString();
      };

      Expr expr = plannerContext.parseExpression(druidExpressions.get(1).getExpression());
      if (druidExpressions.get(0).isSimpleExtraction() && expr.isLiteral()) {
        Object[] lit = expr.eval(InputBindings.nilBindings()).asArray();
        if (lit == null || lit.length == 0) {
          return null;
        }
        HashSet<String> literals = Sets.newHashSetWithExpectedSize(lit.length);
        for (Object o : lit) {
          literals.add(Evals.asString(o));
        }

        DruidExpression druidExpression = DruidExpression.ofVirtualColumn(
            Calcites.getColumnTypeForRelDataType(rexNode.getType()),
            builder,
            druidExpressions,
            (name, outputType, expression, macroTable) -> new ListFilteredVirtualColumn(
                name,
                druidExpressions.get(0)
                                .getSimpleExtraction()
                                .toDimensionSpec(druidExpressions.get(0).getDirectColumn(), outputType),
                literals,
                isAllowList()
            )
        );

        // if the join expression VC registry is present, it means that this expression is part of a join condition
        // and since that's the case, create virtual column here itself for optimized usage in join matching
        if (plannerContext.getJoinExpressionVirtualColumnRegistry() != null) {
          String virtualColumnName = plannerContext.getJoinExpressionVirtualColumnRegistry()
                                                   .getOrCreateVirtualColumnForExpression(
                                                       druidExpression,
                                                       ColumnType.STRING
                                                   );
          return DruidExpression.ofColumn(ColumnType.STRING, virtualColumnName);
        }

        return druidExpression;
      }

      return DruidExpression.ofExpression(ColumnType.STRING, builder, druidExpressions);
    }
  }

  public static class RegexFilter implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_FILTER_REGEX")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_FILTER_REGEX(string, pattern)'",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.family(SqlTypeFamily.CHARACTER)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeCascadeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlFunction calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      final RexCall call = (RexCall) rexNode;
      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands()
      );

      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      final String pattern = RexLiteral.stringValue(call.getOperands().get(1));
      if (pattern == null) {
        return null;
      }
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "filter((x) -> regexp_like(x, \"" + pattern + "\"), " + args.get(0).getExpression() + ")";
      if (druidExpressions.get(0).isSimpleExtraction()) {
        DruidExpression druidExpression = DruidExpression.ofVirtualColumn(
            Calcites.getColumnTypeForRelDataType(rexNode.getType()),
            builder,
            druidExpressions,
            (name, outputType, expression, macroTable) -> new RegexFilteredVirtualColumn(
                name,
                druidExpressions.get(0)
                                .getSimpleExtraction()
                                .toDimensionSpec(druidExpressions.get(0).getDirectColumn(), outputType),
                pattern
            )
        );

        // If in a join context, create the VC immediately
        if (plannerContext.getJoinExpressionVirtualColumnRegistry() != null) {
          String virtualColumnName = plannerContext.getJoinExpressionVirtualColumnRegistry()
                                                   .getOrCreateVirtualColumnForExpression(
                                                       druidExpression,
                                                       ColumnType.STRING
                                                   );
          return DruidExpression.ofColumn(ColumnType.STRING, virtualColumnName);
        }

        return druidExpression;
      }

      return DruidExpression.ofExpression(ColumnType.STRING, builder, druidExpressions);
    }
  }

  public static class PrefixFilter implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_FILTER_PREFIX")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_FILTER_PREFIX(string, prefix)'",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.family(SqlTypeFamily.CHARACTER)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeCascadeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlFunction calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      final RexCall call = (RexCall) rexNode;
      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands()
      );

      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      RexNode prefixNode = call.getOperands().get(1);
      if (!(prefixNode instanceof RexLiteral)) {
        return null;
      }
      DruidLiteral prefixLiteral = Expressions.calciteLiteralToDruidLiteral(plannerContext, prefixNode);
      if (prefixLiteral == null || prefixLiteral.value() == null) {
        return null;
      }
      String prefix = (String) prefixLiteral.value();
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "filter((x) -> (x != null && substring(x, 0, " + prefix.length() + ") == \"" + prefix + "\"), " + args.get(0).getExpression() + ")";

      if (druidExpressions.get(0).isSimpleExtraction()) {
        DruidExpression druidExpression = DruidExpression.ofVirtualColumn(
            Calcites.getColumnTypeForRelDataType(rexNode.getType()),
            builder,
            druidExpressions,
            (name, outputType, expression, macroTable) -> new PrefixFilteredVirtualColumn(
                name,
                druidExpressions.get(0)
                                .getSimpleExtraction()
                                .toDimensionSpec(druidExpressions.get(0).getDirectColumn(), outputType),
                prefix
            )
        );

        // If in a join context, create the VC immediately
        if (plannerContext.getJoinExpressionVirtualColumnRegistry() != null) {
          String virtualColumnName = plannerContext.getJoinExpressionVirtualColumnRegistry()
                                                   .getOrCreateVirtualColumnForExpression(
                                                       druidExpression,
                                                       ColumnType.STRING
                                                   );
          return DruidExpression.ofColumn(ColumnType.STRING, virtualColumnName);
        }

        return druidExpression;
      }

      return DruidExpression.ofExpression(ColumnType.STRING, builder, druidExpressions);
    }
  }

  public static class FilterOnly extends ListFilter
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_FILTER_ONLY")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_FILTER_ONLY(string, array)'",
                OperandTypes.family(SqlTypeFamily.STRING),
                OperandTypes.family(SqlTypeFamily.ARRAY)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeCascadeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }


    @Override
    boolean isAllowList()
    {
      return true;
    }
  }

  public static class FilterNone extends ListFilter
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_FILTER_NONE")
        .operandTypeChecker(
            OperandTypes.sequence(
                "'MV_FILTER_NONE(string, array)'",
                OperandTypes.family(SqlTypeFamily.STRING),
                OperandTypes.family(SqlTypeFamily.ARRAY)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeCascadeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }


    @Override
    boolean isAllowList()
    {
      return false;
    }
  }


  private MultiValueStringOperatorConversions()
  {
    // no instantiation
  }
}
