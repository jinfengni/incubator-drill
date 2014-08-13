/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.planner.sql.rewriter;

import com.google.common.collect.Lists;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlDataTypeSpec;
import org.eigenbase.sql.SqlDynamicParam;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.util.SqlVisitor;

import java.util.List;


public class SqlIdentifierFinder implements SqlVisitor<List<SqlIdentifier>> {
  //~ Methods ----------------------------------------------------------------

  //private static INSTANCE = new SqlIdentifierFinder();

  private static final List<SqlIdentifier> EMPTY_LIST = Lists.newArrayList();

  public static List<SqlIdentifier> findSqlIdentifier(SqlNode root) {
    SqlIdentifierFinder visitor = new SqlIdentifierFinder();
    return root.accept(visitor);
  }

  public List<SqlIdentifier> visit(SqlLiteral literal) {
    return EMPTY_LIST;
  }

  public List<SqlIdentifier> visit(SqlCall call) {
    List<SqlIdentifier> list = Lists.newArrayList();

    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;
      if (select.getSelectList() != null) {
        list.addAll(select.getSelectList().accept(this));
      }

      if (select.hasOrderBy()) {
        list.addAll(select.getOrderList().accept(this));
      }

      if (select.hasWhere()) {
        list.addAll(select.getWhere().accept(this));
      }

      if (select.getHaving() != null) {
        list.addAll((select.getHaving().accept(this)));
      }

      if (select.getGroup() != null) {
        list.addAll((select.getGroup().accept(this)));
      }
    }
    for (SqlNode operand : call.getOperandList()) {
      if (operand == null) {
        continue;
      }
      List<SqlIdentifier> opList = operand.accept(this);

      list.addAll(opList);
    }

    return list;
  }

  public List<SqlIdentifier> visit(SqlNodeList nodeList) {
    List<SqlIdentifier> result = Lists.newArrayList();
    for (int i = 0; i < nodeList.size(); i++) {
      SqlNode node = nodeList.get(i);
      result.addAll(node.accept(this));
    }
    return result;
  }

  public List<SqlIdentifier> visit(SqlIdentifier id) {
    List<SqlIdentifier> list = Lists.newArrayList();
    list.add(id);
    return list;
  }

  public List<SqlIdentifier> visit(SqlDataTypeSpec type) {
    return EMPTY_LIST;
  }

  public List<SqlIdentifier> visit(SqlDynamicParam param) {
    return EMPTY_LIST;
  }

  public List<SqlIdentifier> visit(SqlIntervalQualifier intervalQualifier) {
    return EMPTY_LIST;
  }

}
