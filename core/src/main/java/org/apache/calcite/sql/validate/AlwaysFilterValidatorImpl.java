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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.TimeFrame;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWithItem;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AlwaysFilterValidatorImpl extends SqlValidatorImpl implements AlwaysFilterValidator  {

  final SqlValidatorCatalogReader catalogReader;

  public AlwaysFilterValidatorImpl(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      Config config) {
    super(opTab, catalogReader, typeFactory, config);
    this.catalogReader = catalogReader;
  }

  @Override public SqlValidatorCatalogReader getCatalogReader() {
    return catalogReader;
  }

  @Override public SqlNode validate(SqlNode topNode) {
    SqlValidatorScope scope = new EmptyScope(this);
    scope = new CatalogScope(scope, ImmutableList.of("CATALOG"));
    SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
    if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
      registerQuery(scope, null, outermostNode, outermostNode, null, false);
    }
    Set<String> alwaysFilterFields = new HashSet<>();
    outermostNode.validateAlwaysFilter(this, scope, alwaysFilterFields);
    if (!alwaysFilterFields.isEmpty()) {
      newAlwaysFilterValidationException(alwaysFilterFields);
    }
    return outermostNode;
  }

  @Override public void validateQueryAlwaysFilter(SqlNode node, SqlValidatorScope scope,
      Set<String> alwaysFilterFields) {
    final SqlValidatorNamespace ns = getNamespaceOrThrow(node, scope);
    validateNamespaceAlwaysFilter(ns, alwaysFilterFields);

  }

  public void validateNamespaceAlwaysFilter(final SqlValidatorNamespace namespace, Set<String> alwaysFilterFields){
    namespace.validateAlwaysFilter(alwaysFilterFields);
  }

  public void validateWithItemAlwaysFilter(SqlWithItem withItem, Set<String> alwaysFilterFields) {
    validateSelect((SqlSelect) withItem.query, alwaysFilterFields);
  }

  private void removeIdentifier(Set<String> alwaysFilterFields, List<String> identifiers) {
    for (String identifier: identifiers) {
      alwaysFilterFields.remove(identifier);
    }
  }

  public RuntimeException newAlwaysFilterValidationException(Set<String> alwaysFilterFields) {
    throw new RuntimeException(
        String.format(
        "SQL statement did not contain filters on the following fields: %s",
        alwaysFilterFields), null);
  }

  @Override public void validateSelect(SqlSelect select, Set<String> alwaysFilterFields) {
    final SelectScope fromScope = (SelectScope) getFromScope(select);

    SqlNode fromNode = select.getFrom();
    if (fromNode != null) {
      validateFromAlwaysFilter(select.getFrom(), fromScope, alwaysFilterFields);
      // Check WHERE, HAVING, ON, USING clauses
      validateClause(select.getWhere(), alwaysFilterFields);
      validateClause(select.getHaving(), alwaysFilterFields);
    }
  }

  private void validateClause(SqlNode node, Set<String> alwaysFilterFields){
    if (node != null) {
      List<SqlIdentifier> sqlIdentifiers = node.collectSqlIdentifiers();
      List<String> identifierNames;
      identifierNames = sqlIdentifiers.stream().map((i)-> i.names.get(1)).collect(Collectors.toList());
      removeIdentifier(alwaysFilterFields, identifierNames);
    }
  }

  @Override public void validateJoin(SqlJoin join, SqlValidatorScope scope, Set<String> alwaysFilterFields) {
    validateFromAlwaysFilter(join.getLeft(), scope, alwaysFilterFields);
    validateFromAlwaysFilter(join.getRight(), scope, alwaysFilterFields);
  }

  protected void validateFromAlwaysFilter(
      SqlNode node,
      SqlValidatorScope scope,
      Set<String> alwaysFilterFields) {
    switch (node.getKind()) {
      case AS:
      case TABLE_REF:
        validateFromAlwaysFilter(((SqlCall) node).operand(0), scope, alwaysFilterFields);
      case JOIN:
        validateJoin((SqlJoin) node, scope, alwaysFilterFields);
      default:
        validateQueryAlwaysFilter(node, scope, alwaysFilterFields);
    }
  }



  @Override public void validateCall(SqlCall call, SqlValidatorScope scope) {

  }

  @Override public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {

  }

  @Override public void validateLiteral(SqlLiteral literal) {

  }

  @Override public void validateIntervalQualifier(SqlIntervalQualifier qualifier) {

  }

  @Override public void validateInsert(SqlInsert insert) {

  }

  @Override public void validateUpdate(SqlUpdate update) {

  }

  @Override public void validateDelete(SqlDelete delete) {

  }

  @Override public void validateMerge(SqlMerge merge) {

  }

  @Override public void validateDataType(SqlDataTypeSpec dataType) {

  }

  @Override public void validateDynamicParam(SqlDynamicParam dynamicParam) {

  }

  @Override public void validateWindow(SqlNode windowOrId, SqlValidatorScope scope, @Nullable SqlCall call) {

  }

  @Override public void validateAggregateParams(SqlCall aggCall, @Nullable SqlNode filter,
      @Nullable SqlNodeList distinctList, @Nullable SqlNodeList orderList,
      SqlValidatorScope scope) {

  }

  @Override public boolean validateModality(SqlSelect select, SqlModality modality, boolean fail) {
    return false;
  }

  @Override public void validateSequenceValue(SqlValidatorScope scope, SqlIdentifier id) {

  }

  @Override public TimeFrame validateTimeFrame(SqlIntervalQualifier intervalQualifier) {
    return null;
  }
}
