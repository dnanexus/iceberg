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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.view.ViewOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

/**
 * Unit tests for the Lake Formation flavour of the Glue existence probe.
 *
 * <p>A cross-account {@code glue:GetTable} is authorized before the table name is resolved, so for
 * a name that does not exist Glue answers with {@link AccessDeniedException} rather than {@link
 * EntityNotFoundException}. These tests pin down that such a denial counts as "not found" when
 * {@link AwsProperties#GLUE_LAKEFORMATION_ENABLED} is set, and is propagated untouched when it is
 * not.
 */
public class TestGlueLakeFormationTableProbe {

  private static final String CATALOG_NAME = "glue";
  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String DATABASE_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of(DATABASE_NAME, TABLE_NAME);
  private static final Schema SCHEMA =
      new Schema(required(1, "id", Types.IntegerType.get(), "unique ID"));

  /** Stands in for the single Glue entry backing the table or view under test. */
  private final AtomicReference<Table> glueTable = new AtomicReference<>();

  /** While set, every {@code getTable} is denied the way cross-account Lake Formation denies it. */
  private final AtomicBoolean denyGetTable = new AtomicBoolean(false);

  private GlueClient glue;

  @BeforeEach
  public void before() {
    glueTable.set(null);
    denyGetTable.set(false);

    glue = Mockito.mock(GlueClient.class);

    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name(DATABASE_NAME).build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));

    Mockito.doAnswer(
            invocation -> {
              if (denyGetTable.get()) {
                throw AccessDeniedException.builder()
                    .message(
                        "User: arn:aws:sts::123456789012:assumed-role/role/session is not "
                            + "authorized to perform: glue:GetTable on resource: "
                            + "arn:aws:glue:us-east-1:210987654321:table/db/tbl because no "
                            + "resource-based policy allows the glue:GetTable action")
                    .build();
              }

              Table current = glueTable.get();
              if (current == null) {
                throw EntityNotFoundException.builder().message("Entity not found").build();
              }

              return GetTableResponse.builder().table(current).build();
            })
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    Mockito.doAnswer(
            invocation -> {
              glueTable.set(
                  toGlueTable(invocation.getArgument(0, CreateTableRequest.class).tableInput()));
              return CreateTableResponse.builder().build();
            })
        .when(glue)
        .createTable(Mockito.any(CreateTableRequest.class));

    Mockito.doAnswer(
            invocation -> {
              glueTable.set(
                  toGlueTable(invocation.getArgument(0, UpdateTableRequest.class).tableInput()));
              return UpdateTableResponse.builder().build();
            })
        .when(glue)
        .updateTable(Mockito.any(UpdateTableRequest.class));

    Mockito.doAnswer(
            invocation -> {
              glueTable.set(null);
              return DeleteTableResponse.builder().build();
            })
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));
  }

  @Test
  public void testTableExistsReportsDeniedNameAsMissingWithLakeFormation() {
    denyGetTable.set(true);

    assertThat(catalog(true).tableExists(IDENTIFIER)).isFalse();
  }

  @Test
  public void testTableExistsPropagatesDenialWithoutLakeFormation() {
    GlueCatalog catalog = catalog(false);
    denyGetTable.set(true);

    assertThatThrownBy(() -> catalog.tableExists(IDENTIFIER))
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
  }

  @Test
  public void testViewExistsReportsDeniedNameAsMissingWithLakeFormation() {
    denyGetTable.set(true);

    assertThat(catalog(true).viewExists(IDENTIFIER)).isFalse();
  }

  @Test
  public void testViewExistsPropagatesDenialWithoutLakeFormation() {
    GlueCatalog catalog = catalog(false);
    denyGetTable.set(true);

    assertThatThrownBy(() -> catalog.viewExists(IDENTIFIER))
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
  }

  @Test
  public void testDropTableReportsDeniedNameAsMissingWithLakeFormation() {
    denyGetTable.set(true);

    assertThat(catalog(true).dropTable(IDENTIFIER, false)).isFalse();
    Mockito.verify(glue, Mockito.never()).deleteTable(Mockito.any(DeleteTableRequest.class));
  }

  @Test
  public void testDropTablePropagatesDenialWithoutLakeFormation() {
    GlueCatalog catalog = catalog(false);
    denyGetTable.set(true);

    assertThatThrownBy(() -> catalog.dropTable(IDENTIFIER, false))
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
  }

  @Test
  public void testDropViewReportsDeniedNameAsMissingWithLakeFormation() {
    denyGetTable.set(true);

    assertThat(catalog(true).dropView(IDENTIFIER)).isFalse();
    Mockito.verify(glue, Mockito.never()).deleteTable(Mockito.any(DeleteTableRequest.class));
  }

  @Test
  public void testDropViewPropagatesDenialWithoutLakeFormation() {
    GlueCatalog catalog = catalog(false);
    denyGetTable.set(true);

    assertThatThrownBy(() -> catalog.dropView(IDENTIFIER))
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
  }

  @Test
  public void testTableRefreshReportsDeniedUnknownNameAsMissingWithLakeFormation() {
    GlueCatalog catalog = catalog(true);
    denyGetTable.set(true);

    assertThat(catalog.newTableOps(IDENTIFIER).current())
        .as("A denied probe for a table that was never loaded must read as a missing table")
        .isNull();
  }

  @Test
  public void testTableRefreshPropagatesDenialWithoutLakeFormation() {
    TableOperations ops = catalog(false).newTableOps(IDENTIFIER);
    denyGetTable.set(true);

    assertThatThrownBy(ops::current)
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
  }

  /**
   * The denial is only known to mean "does not exist" for a name that was never resolved. Once the
   * table has been loaded, a later denial is reported as a vanished table, which is the same
   * outcome Glue produces when the entry is dropped or access to it is revoked.
   */
  @Test
  public void testTableRefreshOfLoadedTableReportsDenialAsVanishedWithLakeFormation() {
    GlueCatalog catalog = catalog(true);
    catalog.createTable(IDENTIFIER, SCHEMA);

    TableOperations ops = catalog.newTableOps(IDENTIFIER);
    assertThat(ops.current()).isNotNull();

    denyGetTable.set(true);

    assertThatThrownBy(ops::refresh)
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Cannot find Glue table");
  }

  @Test
  public void testViewRefreshReportsDeniedUnknownNameAsMissingWithLakeFormation() {
    GlueCatalog catalog = catalog(true);
    denyGetTable.set(true);

    assertThat(catalog.newViewOps(IDENTIFIER).current())
        .as("A denied probe for a view that was never loaded must read as a missing view")
        .isNull();
  }

  @Test
  public void testViewRefreshPropagatesDenialWithoutLakeFormation() {
    ViewOperations ops = catalog(false).newViewOps(IDENTIFIER);
    denyGetTable.set(true);

    assertThatThrownBy(ops::current)
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
  }

  /**
   * The end-to-end shape of the problem this behavior exists for: the probe that precedes the
   * create is denied for the whole duration of the create, and the table is still created.
   */
  @Test
  public void testCreateTableSucceedsWhileEveryProbeIsDeniedWithLakeFormation() {
    GlueCatalog catalog = catalog(true);
    denyGetTable.set(true);

    catalog.createTable(IDENTIFIER, SCHEMA);

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    Mockito.verify(glue, Mockito.atLeastOnce()).createTable(captor.capture());
    TableInput created = captor.getValue().tableInput();
    assertThat(created.name()).isEqualTo(TABLE_NAME);
    assertThat(created.parameters())
        .containsEntry(
            BaseMetastoreTableOperations.TABLE_TYPE_PROP,
            BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ROOT))
        .containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
  }

  @Test
  public void testCreateTableFailsOnDeniedProbeWithoutLakeFormation() {
    GlueCatalog catalog = catalog(false);
    denyGetTable.set(true);

    assertThatThrownBy(() -> catalog.createTable(IDENTIFIER, SCHEMA))
        .as("Without Lake Formation the denial has no alternative reading and must surface")
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
  }

  @Test
  public void testExistingTableIsStillFoundWithLakeFormation() {
    GlueCatalog catalog = catalog(true);
    catalog.createTable(IDENTIFIER, SCHEMA);

    assertThat(catalog.tableExists(IDENTIFIER)).isTrue();
    assertThat(catalog.newTableOps(IDENTIFIER).current()).isNotNull();
  }

  private GlueCatalog catalog(boolean lakeFormationEnabled) {
    GlueCatalog catalog = new GlueCatalog();
    catalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(
            ImmutableMap.of(
                AwsProperties.GLUE_LAKEFORMATION_ENABLED, String.valueOf(lakeFormationEnabled))),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
    return catalog;
  }

  private static Table toGlueTable(TableInput tableInput) {
    return Table.builder()
        .name(tableInput.name())
        .tableType(tableInput.tableType())
        .parameters(tableInput.parameters())
        .storageDescriptor(tableInput.storageDescriptor())
        .viewOriginalText(tableInput.viewOriginalText())
        .viewExpandedText(tableInput.viewExpandedText())
        .description(tableInput.description())
        .build();
  }
}
