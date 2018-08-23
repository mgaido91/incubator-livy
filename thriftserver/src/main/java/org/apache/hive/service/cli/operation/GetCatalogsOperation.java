/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.operation;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hive.service.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GetCatalogsOperation.
 *
 */
public class GetCatalogsOperation extends MetadataOperation {

  private static final Logger LOG = LoggerFactory.getLogger(GetCatalogsOperation.class.getName());

  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addStringColumn("TABLE_CAT", "Catalog name. NULL if not applicable.");

  private final RowSet rowSet;

  public GetCatalogsOperation(SessionHandle sessionHandle) {
    super(sessionHandle, OperationType.GET_CATALOGS);
    rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion(), false);
    LOG.info("Starting GetCatalogsOperation");
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      // catalogs are actually not supported in hive, so this is a no-op
      setState(OperationState.FINISHED);
      LOG.info("Fetching catalog metadata has been successfully finished");
    } catch (HiveSQLException e) {
      setState(OperationState.ERROR);
      throw e;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getResultSetSchema()
   */
  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    return RESULT_SET_SCHEMA;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0);
    }
    return rowSet.extractSubset((int)maxRows);
  }
}
