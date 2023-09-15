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
package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.MetaImpl.MetaColumn;
import org.apache.calcite.schema.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Factory for creating MetaColumns for getColumns(). */
public interface CalciteMetaColumnFactory {
  /** Instantiates a MetaColumn. */
  MetaColumn newMetaColumn(
      Table table,
      String tableCat,
      String tableSchem,
      String tableName,
      String columnName,
      int dataType,
      String typeName,
      Integer columnSize,
      @Nullable Integer decimalDigits,
      Integer numPrecRadix,
      int nullable,
      Integer charOctetLength,
      int ordinalPosition,
      String isNullable);

  /** Returns the list of expected column names. */
  String[] getColumnNames();

  /** Returns the class of MetaColumn that is created. */
  Class<?> getMetaColumnClass();
}
