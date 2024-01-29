/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.validate;

import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.TimeFrame;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface AlwaysFilterValidator {

  SqlValidatorCatalogReader getCatalogReader();


  SqlNode validate(SqlNode topNode);

  void validateQuery(SqlNode node, SqlValidatorScope scope,
      RelDataType targetRowType, List<String> alwaysFilters);

  void validateQueryAlwaysFilter(SqlNode node, SqlValidatorScope scope,
      Set<String> alwaysFilterFields);

  void validateCall(
      SqlCall call,
      SqlValidatorScope scope);

  void validateSelect(SqlSelect select, Set<String> alwaysFilterFields);

  List<String> sqlIdentifierToString (List<SqlIdentifier> alwaysFilterFields);

  void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope);
  void validateLiteral(SqlLiteral literal);
  void validateIntervalQualifier(SqlIntervalQualifier qualifier);
  void validateInsert(SqlInsert insert);
  void validateUpdate(SqlUpdate update);
  void validateDelete(SqlDelete delete);
  void validateMerge(SqlMerge merge);
  void validateDataType(SqlDataTypeSpec dataType);
  void validateDynamicParam(SqlDynamicParam dynamicParam);

  void validateWindow(
      SqlNode windowOrId,
      SqlValidatorScope scope,
      @Nullable SqlCall call);

  void validateAggregateParams(SqlCall aggCall, @Nullable SqlNode filter,
      @Nullable SqlNodeList distinctList, @Nullable SqlNodeList orderList,
      SqlValidatorScope scope);

  boolean validateModality(SqlSelect select, SqlModality modality,
      boolean fail);

  void validateWith(SqlWith with, SqlValidatorScope scope);

  void validateWithItem(SqlWithItem withItem);

  void validateSequenceValue(SqlValidatorScope scope, SqlIdentifier id);

  TimeFrame validateTimeFrame(SqlIntervalQualifier intervalQualifier);
}
