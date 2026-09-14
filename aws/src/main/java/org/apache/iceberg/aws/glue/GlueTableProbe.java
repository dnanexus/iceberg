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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Existence probe for Glue tables and views that understands cross-account Lake Formation
 * semantics.
 *
 * <p>A cross-account {@code glue:GetTable} request is authorized before the table name is resolved,
 * and that pre-resolution check can only be satisfied by a Glue Data Catalog resource policy in the
 * owning account. Lake Formation grants apply to resources that exist, so for a name that does not
 * exist yet no grant can satisfy the check and Glue returns {@link AccessDeniedException} instead
 * of {@link EntityNotFoundException}. When {@link AwsProperties#GLUE_LAKEFORMATION_ENABLED} is set,
 * that denial is therefore treated as "not found" so the probe-then-create flow works without a
 * resource policy.
 */
class GlueTableProbe {

  private static final Logger LOG = LoggerFactory.getLogger(GlueTableProbe.class);

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

      LOG.warn(
          "Treating access denied while looking up Glue table {}.{} as not found because {} is "
              + "enabled: cross-account Lake Formation denies glue:GetTable for names that do not "
              + "exist instead of reporting them as missing",
          databaseName,
          tableName,
          AwsProperties.GLUE_LAKEFORMATION_ENABLED,
          e);
      return null;
    }
  }
}
