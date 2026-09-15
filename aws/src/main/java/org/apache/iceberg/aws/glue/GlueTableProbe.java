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
package org.apache.iceberg.aws.glue;

import org.apache.iceberg.aws.AwsProperties;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Existence probe for Glue tables and views that understands Lake Formation and with few
 * cross-account specialities. In case of Lake Formation, it can happen that `glue:GetTable` call
 * will not pass and reject with `AccessDeniedException`. For cross-account configuration this is
 * expected behavior and the commands like `CREATE TABLE IF NOT EXISTS` in such setup cannot probe
 * easily.
 */
class GlueTableProbe {

  private GlueTableProbe() {}

  /**
   * Looks up a Glue table, returning null when it does not exist.
   *
   * @param glue Glue client
   * @param awsProperties AWS properties, used for the catalog id and the Lake Formation flag
   * @param databaseName Glue database name
   * @param tableName Glue table name
   * @return the Glue table, or null if it does not exist. With Lake Formation enabled, an {@link
   *     AccessDeniedException} is also reported as not existing (see the class documentation),
   *     otherwise it is propagated.
   */
  static Table getTableOrNull(
      GlueClient glue, AwsProperties awsProperties, String databaseName, String tableName) {
    try {
      return glue.getTable(
              GetTableRequest.builder()
                  .catalogId(awsProperties.glueCatalogId())
                  .databaseName(databaseName)
                  .name(tableName)
                  .build())
          .table();
    } catch (EntityNotFoundException e) {
      return null;
    } catch (AccessDeniedException e) {
      if (!awsProperties.glueLakeFormationEnabled()) {
        throw e;
      }

      return null;
    }
  }
}
