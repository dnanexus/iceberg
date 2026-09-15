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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;

public class TestGlueCatalog {

  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String CATALOG_NAME = "glue";
  private GlueClient glue;
  private GlueCatalog glueCatalog;

  @BeforeEach
  public void before() {
    glue = Mockito.mock(GlueClient.class);
    glueCatalog = new GlueCatalog();
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
  }

  @Test
  public void testConstructorEmptyWarehousePath() {
    GlueCatalog catalog = new GlueCatalog();
    catalog.initialize(
        CATALOG_NAME,
        null,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doThrow(EntityNotFoundException.builder().build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    assertThatThrownBy(() -> catalog.createTable(TableIdentifier.of("db", "table"), new Schema()))
        .hasMessageContaining(
            "Cannot derive default warehouse location, warehouse path must not be null or empty")
        .isInstanceOf(ValidationException.class);
  }

  @Test
  public void testConstructorWarehousePathWithEndSlash() {
    GlueCatalog catalogWithSlash = new GlueCatalog();
    catalogWithSlash.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH + "/",
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = catalogWithSlash.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    assertThat(location).isEqualTo(WAREHOUSE_PATH + "/db.db/table");
  }

  @Test
  public void testDefaultWarehouseLocationNoDbUri() {
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = glueCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    assertThat(location).isEqualTo(WAREHOUSE_PATH + "/db.db/table");
  }

  @Test
  public void testDefaultWarehouseLocationDbUri() {
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db").locationUri("s3://bucket2/db").build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = glueCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    assertThat(location).isEqualTo("s3://bucket2/db/table");
  }

  @Test
  public void testDefaultWarehouseLocationDbUriTrailingSlash() {
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db").locationUri("s3://bucket2/db/").build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = glueCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));

    assertThat(location).isEqualTo("s3://bucket2/db/table");
  }

  @Test
  public void testDefaultWarehouseLocationCustomCatalogId() {
    GlueCatalog catalogWithCustomCatalogId = new GlueCatalog();
    String catalogId = "myCatalogId";
    AwsProperties awsProperties = new AwsProperties();
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    awsProperties.setGlueCatalogId(catalogId);
    catalogWithCustomCatalogId.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH + "/",
        awsProperties,
        s3FileIOProperties,
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());

    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db").locationUri("s3://bucket2/db").build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    catalogWithCustomCatalogId.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    Mockito.verify(glue)
        .getDatabase(
            Mockito.argThat((GetDatabaseRequest req) -> req.catalogId().equals(catalogId)));
  }

  @Test
  public void testDefaultWarehouseLocationUnique() {
    GlueCatalog catalog = new GlueCatalog();
    catalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        true /* uniqTableLocation */);

    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db").locationUri("s3://bucket2/db").build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = catalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    assertThat(location).matches("s3://bucket2/db/table-[a-z0-9]{32}");
  }

  @Test
  public void testListTables() {
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(
            GetTablesResponse.builder()
                .tableList(
                    Table.builder()
                        .databaseName("db1")
                        .name("t1")
                        .parameters(
                            ImmutableMap.of(
                                BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                        .build(),
                    Table.builder()
                        .databaseName("db1")
                        .name("t2")
                        .parameters(
                            ImmutableMap.of(
                                "key",
                                "val",
                                BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                        .build(),
                    Table.builder()
                        .databaseName("db1")
                        .name("t3")
                        .parameters(
                            ImmutableMap.of(
                                "key",
                                "val",
                                BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                "wrongVal"))
                        .build(),
                    Table.builder()
                        .databaseName("db1")
                        .name("t4")
                        .parameters(ImmutableMap.of("key", "val"))
                        .build(),
                    Table.builder().databaseName("db1").name("t5").parameters(null).build())
                .build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    assertThat(glueCatalog.listTables(Namespace.of("db1")))
        .isEqualTo(
            Lists.newArrayList(TableIdentifier.of("db1", "t1"), TableIdentifier.of("db1", "t2")));
  }

  @Test
  public void testListTablesPagination() {
    AtomicInteger counter = new AtomicInteger(10);
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                if (counter.decrementAndGet() > 0) {
                  return GetTablesResponse.builder()
                      .tableList(
                          Table.builder()
                              .databaseName("db1")
                              .name(UUID.randomUUID().toString().replace("-", ""))
                              .parameters(
                                  ImmutableMap.of(
                                      BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                      BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                              .build())
                      .nextToken("token")
                      .build();
                } else {
                  return GetTablesResponse.builder()
                      .tableList(
                          Table.builder()
                              .databaseName("db1")
                              .name("tb1")
                              .parameters(
                                  ImmutableMap.of(
                                      BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                      BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                              .build())
                      .build();
                }
              }
            })
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    assertThat(glueCatalog.listTables(Namespace.of("db1"))).hasSize(10);
  }

  @Test
  public void testDropTable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);
    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder().databaseName("db1").name("t1").parameters(properties).build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));
    glueCatalog.dropTable(TableIdentifier.of("db1", "t1"));
  }

  @Test
  public void testDropTableButItsView() {
    TableIdentifier viewIdent = TableIdentifier.of("db", "as_view");
    Table glueView =
        Table.builder()
            .databaseName("db")
            .name("as_view")
            .tableType("VIRTUAL_VIEW")
            .parameters(ImmutableMap.of("table_type", "iceberg-view"))
            .build();

    Mockito.doReturn(GetTableResponse.builder().table(glueView).build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    // purge = false is the mode renameTable uses, purge = true is the Catalog default
    assertThat(glueCatalog.dropTable(viewIdent, false)).isFalse();
    assertThat(glueCatalog.dropTable(viewIdent)).isFalse();

    Mockito.verify(glue, Mockito.never()).deleteTable(Mockito.any(DeleteTableRequest.class));
  }

  @Test
  public void testRenameTable() {
    AtomicInteger counter = new AtomicInteger(1);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);
    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder().databaseName("db1").name("t1").parameters(properties).build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(GetTablesResponse.builder().build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                counter.decrementAndGet();
                return DeleteTableResponse.builder().build();
              }
            })
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));
    glueCatalog.dropTable(TableIdentifier.of("db1", "t1"));
    assertThat(counter.get()).isEqualTo(0);
  }

  @Test
  public void testRenameTableButItsView() {
    TableIdentifier fromIdent = TableIdentifier.of("db", "as_view");
    TableIdentifier toIdent = TableIdentifier.of("db", "renamed_view");
    Table glueView =
        Table.builder()
            .databaseName("db")
            .name("as_view")
            .tableType("VIRTUAL_VIEW")
            .parameters(ImmutableMap.of("table_type", "iceberg-view"))
            .build();

    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(GetTableResponse.builder().table(glueView).build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    assertThatThrownBy(() -> glueCatalog.renameTable(fromIdent, toIdent))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("is not an Iceberg table in Glue");

    // the destination must not be created, otherwise two Glue entries share one metadata file
    Mockito.verify(glue, Mockito.never()).createTable(Mockito.any(CreateTableRequest.class));
    Mockito.verify(glue, Mockito.never()).deleteTable(Mockito.any(DeleteTableRequest.class));
  }

  @Test
  public void testRenameTableWithStorageDescriptor() {
    AtomicInteger counter = new AtomicInteger(1);

    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);

    Map<String, String> storageDescriptorParameters = Maps.newHashMap();
    storageDescriptorParameters.put("key_0", "value_0");

    StorageDescriptor storageDescriptor =
        StorageDescriptor.builder().parameters(storageDescriptorParameters).build();

    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder()
                        .databaseName("db")
                        .name("t_renamed")
                        .parameters(parameters)
                        .storageDescriptor(storageDescriptor)
                        .build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(GetTablesResponse.builder().build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));

    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                CreateTableRequest createTableRequest =
                    (CreateTableRequest) invocation.getArguments()[0];
                if (createTableRequest.tableInput().storageDescriptor().hasParameters()) {
                  counter.decrementAndGet();
                }
                return CreateTableResponse.builder().build();
              }
            })
        .when(glue)
        .createTable(Mockito.any(CreateTableRequest.class));

    glueCatalog.renameTable(TableIdentifier.of("db", "t"), TableIdentifier.of("db", "x_renamed"));
    assertThat(counter.get()).isEqualTo(0);
  }

  @Test
  public void testCreateNamespace() {
    Mockito.doReturn(CreateDatabaseResponse.builder().build())
        .when(glue)
        .createDatabase(Mockito.any(CreateDatabaseRequest.class));
    glueCatalog.createNamespace(Namespace.of("db"));
  }

  @Test
  public void testCreateNamespaceBadName() {
    Mockito.doReturn(CreateDatabaseResponse.builder().build())
        .when(glue)
        .createDatabase(Mockito.any(CreateDatabaseRequest.class));
    List<Namespace> invalidNamespaces =
        Lists.newArrayList(Namespace.of("db-1"), Namespace.of("db", "db2"));

    for (Namespace namespace : invalidNamespaces) {
      assertThatThrownBy(() -> glueCatalog.createNamespace(namespace))
          .isInstanceOf(ValidationException.class)
          .hasMessageStartingWith("Cannot convert namespace")
          .hasMessageEndingWith(
              "to Glue database name, "
                  + "because it must be 1-252 chars of lowercase letters, numbers, underscore");
    }
  }

  @Test
  public void testListAllNamespaces() {
    Mockito.doReturn(
            GetDatabasesResponse.builder()
                .databaseList(
                    Database.builder().name("db1").build(), Database.builder().name("db2").build())
                .build())
        .when(glue)
        .getDatabases(Mockito.any(GetDatabasesRequest.class));
    assertThat(glueCatalog.listNamespaces())
        .isEqualTo(Lists.newArrayList(Namespace.of("db1"), Namespace.of("db2")));
  }

  @Test
  public void testListNamespacesPagination() {
    AtomicInteger counter = new AtomicInteger(10);
    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                if (counter.decrementAndGet() > 0) {
                  return GetDatabasesResponse.builder()
                      .databaseList(
                          Database.builder()
                              .name(UUID.randomUUID().toString().replace("-", ""))
                              .build())
                      .nextToken("token")
                      .build();
                } else {
                  return GetDatabasesResponse.builder()
                      .databaseList(Database.builder().name("db").build())
                      .build();
                }
              }
            })
        .when(glue)
        .getDatabases(Mockito.any(GetDatabasesRequest.class));
    assertThat(glueCatalog.listNamespaces()).hasSize(10);
  }

  @Test
  public void testListNamespacesWithNameShouldReturnItself() {
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    assertThat(glueCatalog.listNamespaces(Namespace.of("db1")))
        .as("list self should return empty list")
        .isEmpty();
  }

  @Test
  public void testListNamespacesBadName() {

    assertThatThrownBy(() -> glueCatalog.listNamespaces(Namespace.of("db-1")))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot convert namespace db-1 to Glue database name, "
                + "because it must be 1-252 chars of lowercase letters, numbers, underscore");
  }

  @Test
  public void testLoadNamespaceMetadata() {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("key", "val");
    parameters.put(IcebergToGlueConverter.GLUE_DB_LOCATION_KEY, "s3://bucket2/db");
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(
                    Database.builder()
                        .name("db1")
                        .parameters(parameters)
                        .locationUri("s3://bucket2/db/")
                        .build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    assertThat(glueCatalog.loadNamespaceMetadata(Namespace.of("db1"))).isEqualTo(parameters);
  }

  @Test
  public void testDropNamespace() {
    Mockito.doReturn(GetTablesResponse.builder().build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue)
        .deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));
    glueCatalog.dropNamespace(Namespace.of("db1"));
  }

  @Test
  public void testDropNamespaceThatContainsOnlyIcebergTable() {
    Mockito.doReturn(
            GetTablesResponse.builder()
                .tableList(
                    Table.builder()
                        .databaseName("db1")
                        .name("t1")
                        .parameters(
                            ImmutableMap.of(
                                BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                        .build())
                .build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue)
        .deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));

    assertThatThrownBy(() -> glueCatalog.dropNamespace(Namespace.of("db1")))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Cannot drop namespace db1 because it still contains Iceberg tables");
  }

  @Test
  public void testDropNamespaceThatContainsNonIcebergTable() {
    Mockito.doReturn(
            GetTablesResponse.builder()
                .tableList(Table.builder().databaseName("db1").name("t1").build())
                .build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue)
        .deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));

    assertThatThrownBy(() -> glueCatalog.dropNamespace(Namespace.of("db1")))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage(
            "Cannot drop namespace db1 because it still contains non-Iceberg tables or views");
  }

  @Test
  public void testSetProperties() {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("key", "val");
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db1").parameters(parameters).build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(UpdateDatabaseResponse.builder().build())
        .when(glue)
        .updateDatabase(Mockito.any(UpdateDatabaseRequest.class));
    glueCatalog.setProperties(Namespace.of("db1"), parameters);
  }

  @Test
  public void testRemoveProperties() {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("key", "val");
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db1").parameters(parameters).build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(UpdateDatabaseResponse.builder().build())
        .when(glue)
        .updateDatabase(Mockito.any(UpdateDatabaseRequest.class));
    glueCatalog.removeProperties(Namespace.of("db1"), Sets.newHashSet("key"));
  }

  @Test
  public void testTablePropsDefinedAtCatalogLevel() {
    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            "table-default.key1", "catalog-default-key1",
            "table-default.key2", "catalog-default-key2",
            "table-default.key3", "catalog-default-key3",
            "table-override.key3", "catalog-override-key3",
            "table-override.key4", "catalog-override-key4");
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        catalogProps);
    Map<String, String> properties = glueCatalog.properties();
    assertThat(properties)
        .isNotEmpty()
        .containsEntry("table-default.key1", "catalog-default-key1")
        .containsEntry("table-default.key2", "catalog-default-key2")
        .containsEntry("table-default.key3", "catalog-default-key3")
        .containsEntry("table-override.key3", "catalog-override-key3")
        .containsEntry("table-override.key4", "catalog-override-key4");
  }

  @Test
  public void testValidateIdentifierSkipNameValidation() {
    AwsProperties props = new AwsProperties();
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    props.setGlueCatalogSkipNameValidation(true);
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        props,
        s3FileIOProperties,
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    assertThat(glueCatalog.isValidIdentifier(TableIdentifier.parse("db-1.a-1"))).isEqualTo(true);
  }

  @Test
  public void testTableLevelS3TagProperties() {
    Map<String, String> properties =
        ImmutableMap.of(
            S3FileIOProperties.WRITE_TABLE_TAG_ENABLED,
            "true",
            S3FileIOProperties.WRITE_NAMESPACE_TAG_ENABLED,
            "true");
    AwsProperties awsProperties = new AwsProperties(properties);
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        awsProperties,
        s3FileIOProperties,
        glue,
        LockManagers.defaultLockManager(),
        properties);
    GlueTableOperations glueTableOperations =
        (GlueTableOperations)
            glueCatalog.newTableOps(TableIdentifier.of(Namespace.of("db"), "table"));
    Map<String, String> tableCatalogProperties = glueTableOperations.tableCatalogProperties();

    assertThat(tableCatalogProperties)
        .containsEntry(
            S3FileIOProperties.WRITE_TAGS_PREFIX.concat(S3FileIOProperties.S3_TAG_ICEBERG_TABLE),
            "table")
        .containsEntry(
            S3FileIOProperties.WRITE_TAGS_PREFIX.concat(
                S3FileIOProperties.S3_TAG_ICEBERG_NAMESPACE),
            "db");
  }

  @Test
  public void testViewLevelLakeFormationProperties() {
    Map<String, String> properties =
        ImmutableMap.of(
            AwsProperties.GLUE_LAKEFORMATION_ENABLED,
            "true",
            CatalogProperties.FILE_IO_IMPL,
            InMemoryFileIO.class.getName());
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        glue,
        LockManagers.defaultLockManager(),
        properties);

    assertThat(
            glueCatalog.viewSpecificCatalogProperties(
                TableIdentifier.of(Namespace.of("db"), "view")))
        .containsEntry(AwsProperties.LAKE_FORMATION_DB_NAME, "db")
        .containsEntry(AwsProperties.LAKE_FORMATION_TABLE_NAME, "view")
        .containsEntry(S3FileIOProperties.PRELOAD_CLIENT_ENABLED, "true");
  }

  @Test
  public void testViewLevelPropertiesWithoutLakeFormation() {
    GlueCatalog catalog = glueCatalogWithInMemoryFileIO();

    assertThat(
            catalog.viewSpecificCatalogProperties(TableIdentifier.of(Namespace.of("db"), "view")))
        .doesNotContainKey(AwsProperties.LAKE_FORMATION_DB_NAME)
        .doesNotContainKey(AwsProperties.LAKE_FORMATION_TABLE_NAME)
        .doesNotContainKey(S3FileIOProperties.PRELOAD_CLIENT_ENABLED)
        .containsEntry(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName());
  }

  @Test
  public void testViewFileIOCachedPerIdentifier() throws IOException {
    GlueCatalog catalog = glueCatalogWithInMemoryFileIO();
    TableIdentifier view1 = TableIdentifier.of(Namespace.of("db"), "view1");
    TableIdentifier view2 = TableIdentifier.of(Namespace.of("db"), "view2");
    GlueViewOperations ops1a = (GlueViewOperations) catalog.newViewOps(view1);
    GlueViewOperations ops1b = (GlueViewOperations) catalog.newViewOps(view1);
    GlueViewOperations ops2 = (GlueViewOperations) catalog.newViewOps(view2);

    // repeated loads of the same view reuse one FileIO, while distinct views get their own so
    // that view-specific properties (e.g. LakeFormation db/table name) can differ between views
    assertThat(ops1a.io()).isSameAs(ops1b.io());
    assertThat(ops1a.io()).isNotSameAs(ops2.io());
    assertThat(catalog.viewFileIOByIdentifier()).hasSize(2);

    catalog.close();
    assertThat(catalog.viewFileIOByIdentifier()).isEmpty();
  }

  @Test
  public void testDropView() {
    // the metadata file is not present in the FileIO, the view must still be dropped
    GlueCatalog catalog = glueCatalogWithInMemoryFileIO();
    TableIdentifier viewIdent = TableIdentifier.of("db", "drop_view");
    Table glueView =
        Table.builder()
            .databaseName("db")
            .name("drop_view")
            .tableType("VIRTUAL_VIEW")
            .parameters(
                ImmutableMap.of(
                    "table_type",
                    "iceberg-view",
                    "metadata_location",
                    WAREHOUSE_PATH + "/db/drop_view/metadata/00001-missing.metadata.json"))
            .build();

    Mockito.doReturn(GetTableResponse.builder().table(glueView).build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));

    boolean dropped = catalog.dropView(viewIdent);
    assertThat(dropped).isTrue();

    Mockito.verify(glue, Mockito.times(1))
        .deleteTable(
            Mockito.argThat(
                (DeleteTableRequest r) ->
                    r.databaseName().equals("db") && r.name().equals("drop_view")));
  }

  @Test
  public void testDropViewDeletesMetadataFile() {
    GlueCatalog catalog = glueCatalogWithInMemoryFileIO();
    InMemoryFileIO io = new InMemoryFileIO();
    String metadataLocation = writeViewMetadata(io, "drop_view_gc", ImmutableMap.of());
    Table glueView =
        Table.builder()
            .databaseName("db")
            .name("drop_view_gc")
            .tableType("VIRTUAL_VIEW")
            .parameters(
                ImmutableMap.of(
                    "table_type", "iceberg-view", "metadata_location", metadataLocation))
            .build();

    Mockito.doReturn(GetTableResponse.builder().table(glueView).build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));

    boolean dropped = catalog.dropView(TableIdentifier.of("db", "drop_view_gc"));
    assertThat(dropped).isTrue();
    assertThat(io.fileExists(metadataLocation)).isFalse();
  }

  @Test
  public void testDropViewKeepsMetadataFileWhenGcDisabled() {
    GlueCatalog catalog = glueCatalogWithInMemoryFileIO();
    InMemoryFileIO io = new InMemoryFileIO();
    String metadataLocation =
        writeViewMetadata(
            io, "drop_view_no_gc", ImmutableMap.of(TableProperties.GC_ENABLED, "false"));
    Table glueView =
        Table.builder()
            .databaseName("db")
            .name("drop_view_no_gc")
            .tableType("VIRTUAL_VIEW")
            .parameters(
                ImmutableMap.of(
                    "table_type", "iceberg-view", "metadata_location", metadataLocation))
            .build();

    Mockito.doReturn(GetTableResponse.builder().table(glueView).build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));

    boolean dropped = catalog.dropView(TableIdentifier.of("db", "drop_view_no_gc"));
    assertThat(dropped).isTrue();
    assertThat(io.fileExists(metadataLocation)).isTrue();
  }

  @Test
  public void testLoadViewButItsNotIcebergView() {
    TableIdentifier viewIdent = TableIdentifier.of("db", "foreign_view");
    Table glueTable =
        Table.builder()
            .databaseName("db")
            .name("foreign_view")
            .tableType("VIRTUAL_VIEW")
            // neither ICEBERG nor iceberg-view, e.g. an entity registered by another engine
            .parameters(ImmutableMap.of("table_type", "DELTA"))
            .build();

    Mockito.doReturn(GetTableResponse.builder().table(glueTable).build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    Throwable thrown = catchThrowable(() -> glueCatalog.loadView(viewIdent));

    // a foreign table_type must leave refresh disabled. otherwise refresh() ends by calling
    // current(), which refreshes again because shouldRefresh was never cleared, and the two
    // recurse until the stack is exhausted
    assertThat(thrown)
        .as("loadView must not recurse between current() and refresh()")
        .isNotInstanceOf(StackOverflowError.class);
    assertThat(thrown)
        .isInstanceOf(NoSuchViewException.class)
        .hasMessageContaining("View does not exist");

    // one doRefresh means one Glue lookup: the recursion issues one per level instead
    Mockito.verify(glue, Mockito.times(1)).getTable(Mockito.any(GetTableRequest.class));
  }

  @Test
  public void testDropViewNotFound() {
    TableIdentifier viewIdent = TableIdentifier.of("db", "no_view");
    Mockito.doThrow(EntityNotFoundException.builder().build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    boolean result = glueCatalog.dropView(viewIdent);
    assertThat(result).isFalse();
  }

  @Test
  public void testDropViewButItsNotView() {
    TableIdentifier viewIdent = TableIdentifier.of("db", "not_view");
    Table glueTable =
        Table.builder()
            .databaseName("db")
            .name("not_view")
            .tableType("EXTERNAL_TABLE")
            .parameters(ImmutableMap.of("table_type", "iceberg-table")) // not "iceberg-view"
            .build();

    Mockito.doReturn(GetTableResponse.builder().table(glueTable).build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    boolean dropped = glueCatalog.dropView(viewIdent);
    assertThat(dropped).isFalse();
  }

  @Test
  public void testListViews() {
    Mockito.doReturn(
            GetTablesResponse.builder()
                .tableList(
                    Table.builder()
                        .databaseName("db")
                        .name("my_view")
                        .tableType("VIRTUAL_VIEW")
                        .parameters(
                            ImmutableMap.of(
                                "table_type",
                                "iceberg-view",
                                "metadata_location",
                                "s3://v1/metadata.json"))
                        .build(),
                    Table.builder()
                        .databaseName("db")
                        .name("my_table")
                        .tableType("EXTERNAL_TABLE")
                        .parameters(ImmutableMap.of("table_type", "iceberg-table"))
                        .build())
                .build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));

    List<TableIdentifier> views = glueCatalog.listViews(Namespace.of("db"));
    assertThat(views).hasSize(1).containsExactly(TableIdentifier.of("db", "my_view"));
  }

  @Test
  public void testRenameView() {
    TableIdentifier from = TableIdentifier.of("db", "old_view");
    TableIdentifier to = TableIdentifier.of("db", "new_view");

    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));

    Table existingView =
        Table.builder()
            .databaseName("db")
            .name("old_view")
            .tableType("VIRTUAL_VIEW")
            .parameters(ImmutableMap.of("table_type", "iceberg-view"))
            .build();
    Mockito.doReturn(GetTableResponse.builder().table(existingView).build())
        .when(glue)
        .getTable(Mockito.<GetTableRequest>argThat(r -> "old_view".equals(r.name())));

    Mockito.doThrow(EntityNotFoundException.builder().build())
        .when(glue)
        .getTable(Mockito.<GetTableRequest>argThat(r -> "new_view".equals(r.name())));

    Mockito.doReturn(CreateTableResponse.builder().build())
        .when(glue)
        .createTable(Mockito.any(CreateTableRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));

    glueCatalog.renameView(from, to);

    Mockito.verify(glue, Mockito.times(1))
        .createTable(
            Mockito.argThat(
                (CreateTableRequest r) ->
                    r.tableInput().name().equals("new_view")
                        && "VIRTUAL_VIEW".equals(r.tableInput().tableType())
                        && "iceberg-view".equals(r.tableInput().parameters().get("table_type"))));
    Mockito.verify(glue, Mockito.times(1))
        .deleteTable(Mockito.argThat((DeleteTableRequest r) -> r.name().equals("old_view")));
  }

  @Test
  public void testRenameViewKeepsMetadataFile() {
    // the renamed view points at the metadata file of the source, so it must not be deleted
    GlueCatalog catalog = glueCatalogWithInMemoryFileIO();
    InMemoryFileIO io = new InMemoryFileIO();
    String metadataLocation = writeViewMetadata(io, "rename_view", ImmutableMap.of());

    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));

    Table existingView =
        Table.builder()
            .databaseName("db")
            .name("rename_view")
            .tableType("VIRTUAL_VIEW")
            .parameters(
                ImmutableMap.of(
                    "table_type", "iceberg-view", "metadata_location", metadataLocation))
            .build();
    Mockito.doReturn(GetTableResponse.builder().table(existingView).build())
        .when(glue)
        .getTable(Mockito.<GetTableRequest>argThat(r -> "rename_view".equals(r.name())));
    Mockito.doThrow(EntityNotFoundException.builder().build())
        .when(glue)
        .getTable(Mockito.<GetTableRequest>argThat(r -> "renamed_view".equals(r.name())));
    Mockito.doReturn(CreateTableResponse.builder().build())
        .when(glue)
        .createTable(Mockito.any(CreateTableRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));

    catalog.renameView(
        TableIdentifier.of("db", "rename_view"), TableIdentifier.of("db", "renamed_view"));

    Mockito.verify(glue, Mockito.times(1))
        .deleteTable(Mockito.argThat((DeleteTableRequest r) -> r.name().equals("rename_view")));
    // the destination must not be rolled back
    Mockito.verify(glue, Mockito.never())
        .deleteTable(Mockito.argThat((DeleteTableRequest r) -> r.name().equals("renamed_view")));
    assertThat(io.fileExists(metadataLocation)).isTrue();
  }

  /**
   * A cross-account {@code glue:GetTable} is authorized before the table name is resolved, so for a
   * name that does not exist Glue answers with {@link AccessDeniedException} rather than {@link
   * EntityNotFoundException}. With Lake Formation enabled that denial has to read as "not found",
   * otherwise no client can ever probe for a table it is about to create.
   */
  @Test
  public void testAccessDeniedReadsAsNotFoundWithLakeFormation() {
    Mockito.doThrow(accessDeniedOnGetTable())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    GlueCatalog catalog = lakeFormationCatalog(null);
    assertThat(catalog.tableExists(TableIdentifier.of("db", "missing"))).isFalse();
    assertThat(catalog.viewExists(TableIdentifier.of("db", "missing"))).isFalse();
    assertThat(catalog.dropTable(TableIdentifier.of("db", "missing"), false)).isFalse();
    assertThat(catalog.dropView(TableIdentifier.of("db", "missing"))).isFalse();
    assertThat(catalog.newTableOps(TableIdentifier.of("db", "missing")).current()).isNull();
  }

  @Test
  public void testAccessDeniedIsPropagatedWithoutLakeFormation() {
    Mockito.doThrow(accessDeniedOnGetTable())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    TableIdentifier identifier = TableIdentifier.of("db", "missing");
    assertThatThrownBy(() -> glueCatalog.tableExists(identifier))
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
    assertThatThrownBy(() -> glueCatalog.viewExists(identifier))
        .isInstanceOf(AccessDeniedException.class)
        .hasMessageContaining("no resource-based policy allows the glue:GetTable action");
  }

  /**
   * The create flow this behavior exists for: the existence probe is denied for the whole duration
   * of the create, and the table is still created. The Lake Formation temporary table stands in for
   * the table being created, so it has to be created in the catalog that will hold the real table
   * rather than in the caller's own catalog.
   */
  @Test
  public void testCreateTableWithLakeFormationWhileProbeIsDenied() {
    Mockito.doThrow(accessDeniedOnGetTable())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(CreateTableResponse.builder().build())
        .when(glue)
        .createTable(Mockito.any(CreateTableRequest.class));

    lakeFormationCatalog("210987654321")
        .createTable(
            TableIdentifier.of("db", "new_table"),
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    Mockito.verify(glue, Mockito.atLeast(2)).createTable(captor.capture());
    assertThat(captor.getAllValues())
        .as("Both the Lake Formation temp table and the real table must name the shared catalog")
        .allSatisfy(request -> assertThat(request.catalogId()).isEqualTo("210987654321"));
    assertThat(captor.getValue().tableInput().parameters())
        .containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
  }

  private static AccessDeniedException accessDeniedOnGetTable() {
    return AccessDeniedException.builder()
        .message(
            "User: arn:aws:sts::123456789012:assumed-role/role/session is not authorized to "
                + "perform: glue:GetTable on resource: arn:aws:glue:us-east-1:210987654321:"
                + "table/db/missing because no resource-based policy allows the glue:GetTable "
                + "action")
        .build();
  }

  private GlueCatalog lakeFormationCatalog(String glueCatalogId) {
    ImmutableMap.Builder<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(AwsProperties.GLUE_LAKEFORMATION_ENABLED, "true");
    if (glueCatalogId != null) {
      properties.put(AwsProperties.GLUE_CATALOG_ID, glueCatalogId);
    }

    GlueCatalog catalog = new GlueCatalog();
    catalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(properties.buildOrThrow()),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
    return catalog;
  }

  private GlueCatalog glueCatalogWithInMemoryFileIO() {
    GlueCatalog catalog = new GlueCatalog();
    catalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
    return catalog;
  }

  /**
   * Writes view metadata for the given view into the in-memory FileIO and returns its location.
   * Locations are derived from the view name and a random UUID because {@link InMemoryFileIO}
   * shares its files between all instances.
   */
  private String writeViewMetadata(
      InMemoryFileIO io, String viewName, Map<String, String> properties) {
    ViewVersion version =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(1234L)
            .schemaId(0)
            .defaultCatalog(CATALOG_NAME)
            .defaultNamespace(Namespace.of("db"))
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder()
                    .sql("select 1 id")
                    .dialect("spark")
                    .build())
            .build();
    ViewMetadata metadata =
        ViewMetadata.builder()
            .addSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .addVersion(version)
            .setLocation(String.format("%s/db/%s", WAREHOUSE_PATH, viewName))
            .setProperties(properties)
            .setCurrentVersionId(1)
            .upgradeFormatVersion(1)
            .build();
    String metadataLocation =
        String.format(
            "%s/db/%s/metadata/00001-%s.metadata.json",
            WAREHOUSE_PATH, viewName, UUID.randomUUID());
    io.addFile(
        metadataLocation, ViewMetadataParser.toJson(metadata).getBytes(StandardCharsets.UTF_8));
    return metadataLocation;
  }
}
