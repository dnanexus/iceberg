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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

/**
 * Unit tests for the commit-failure handling in {@link GlueViewOperations}, backed by a mocked
 * {@link GlueClient} and an in-memory {@link org.apache.iceberg.io.FileIO} so that no AWS
 * credentials are required.
 */
public class TestGlueViewCommitFailure {

  private static final String CATALOG_NAME = "glue";
  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String DATABASE_NAME = "db";
  private static final String VIEW_NAME = "view";

  /** Stands in for the producing account that owns the shared database. */
  private static final String GLUE_CATALOG_ID = "210987654321";

  private static final TableIdentifier VIEW_IDENTIFIER =
      TableIdentifier.of(DATABASE_NAME, VIEW_NAME);
  private static final Schema SCHEMA =
      new Schema(required(1, "id", Types.IntegerType.get(), "unique ID"));

  /** Stands in for the single Glue table backing the view under test. */
  private final AtomicReference<Table> glueView = new AtomicReference<>();

  /** Flipped on once a commit has failed, so that commit status checks can be made to fail. */
  private final AtomicBoolean failStatusChecks = new AtomicBoolean(false);

  /** Counts how many times a commit status check reached Glue. */
  private final AtomicInteger statusCheckAttempts = new AtomicInteger(0);

  private GlueClient glue;
  private GlueCatalog glueCatalog;

  @BeforeEach
  public void before() {
    glueView.set(null);
    failStatusChecks.set(false);
    statusCheckAttempts.set(0);

    glue = Mockito.mock(GlueClient.class);

    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name(DATABASE_NAME).build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));

    Mockito.doAnswer(
            invocation -> {
              if (failStatusChecks.get()) {
                statusCheckAttempts.incrementAndGet();
                // deliberately not EntityNotFoundException, so that GlueViewOperations propagates
                // it and the retry wrapper around the status check gets a chance to retry
                throw GlueException.builder()
                    .message("Glue is unreachable")
                    .statusCode(500)
                    .build();
              }

              Table current = glueView.get();
              if (current == null) {
                throw EntityNotFoundException.builder().message("Entity not found").build();
              }

              return GetTableResponse.builder().table(current).build();
            })
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    Mockito.doAnswer(
            invocation -> {
              glueView.set(
                  toGlueTable(invocation.getArgument(0, CreateTableRequest.class).tableInput()));
              return CreateTableResponse.builder().build();
            })
        .when(glue)
        .createTable(Mockito.any(CreateTableRequest.class));

    Mockito.doAnswer(
            invocation -> {
              glueView.set(
                  toGlueTable(invocation.getArgument(0, UpdateTableRequest.class).tableInput()));
              return UpdateTableResponse.builder().build();
            })
        .when(glue)
        .updateTable(Mockito.any(UpdateTableRequest.class));

    glueCatalog = new GlueCatalog();
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
  }

  /**
   * The commit request never reached Glue, but the status check cannot prove that: it only sees
   * that the current pointer is not ours, which is equally consistent with a lost response or with
   * a second committer having landed a newer commit on top of ours. The outcome must therefore be
   * unknown, and the new metadata file must be left in place.
   */
  @Test
  public void testFailedCommitThrowsUnknownExceptionAndRetainsMetadataFile() {
    GlueViewOperations ops = createView();
    ViewMetadata base = ops.current();

    Mockito.doThrow(new RuntimeException("Datacenter on fire"))
        .when(glue)
        .updateTable(Mockito.any(UpdateTableRequest.class));

    assertThatThrownBy(() -> ops.commit(base, updatedMetadata(base)))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageContaining("Datacenter on fire");

    String newMetadataLocation = attemptedMetadataLocation();
    assertThat(newMetadataLocation)
        .as("A new metadata file should have been written for the commit attempt")
        .isNotNull()
        .isNotEqualTo(base.metadataFileLocation());
    assertThat(fileIO(ops).fileExists(newMetadataLocation))
        .as("Commit outcome is unknown, so the new metadata file must not be deleted")
        .isTrue();
    assertThat(fileIO(ops).fileExists(base.metadataFileLocation()))
        .as("The base metadata file must be untouched")
        .isTrue();
  }

  /**
   * A clean {@link ConcurrentModificationException} with no SDK-level retry is proof that the
   * commit never happened, so the status check is skipped, the failure is definitive, and the
   * orphaned metadata file is cleaned up.
   */
  @Test
  public void testConcurrentModificationExceptionFailsCommitAndDeletesMetadataFile() {
    GlueViewOperations ops = createView();
    ViewMetadata base = ops.current();

    Mockito.doThrow(ConcurrentModificationException.builder().message("concurrent update").build())
        .when(glue)
        .updateTable(Mockito.any(UpdateTableRequest.class));

    assertThatThrownBy(() -> ops.commit(base, updatedMetadata(base)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Glue detected concurrent update")
        .cause()
        .isInstanceOf(ConcurrentModificationException.class);

    String newMetadataLocation = attemptedMetadataLocation();
    assertThat(newMetadataLocation).isNotNull();
    assertThat(fileIO(ops).fileExists(newMetadataLocation))
        .as("Commit definitively failed, so the new metadata file should be cleaned up")
        .isFalse();
    assertThat(fileIO(ops).fileExists(base.metadataFileLocation()))
        .as("The base metadata file must be untouched")
        .isTrue();
  }

  /**
   * The commit landed in Glue but the response was lost. The status check sees our own pointer and
   * reports success, so the commit must not be reported as a failure and the metadata file must
   * survive.
   */
  @Test
  public void testCommitSucceededButResponseWasLost() {
    GlueViewOperations ops = createView();
    ViewMetadata base = ops.current();

    Mockito.doAnswer(
            invocation -> {
              glueView.set(
                  toGlueTable(invocation.getArgument(0, UpdateTableRequest.class).tableInput()));
              throw new RuntimeException("Datacenter on fire");
            })
        .when(glue)
        .updateTable(Mockito.any(UpdateTableRequest.class));

    ops.commit(base, updatedMetadata(base));

    String newMetadataLocation = attemptedMetadataLocation();
    assertThat(newMetadataLocation).isNotNull();
    assertThat(fileIO(ops).fileExists(newMetadataLocation))
        .as("Commit actually succeeded, so the new metadata file must be retained")
        .isTrue();
    assertThat(glueView.get().parameters())
        .containsEntry(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, newMetadataLocation);
  }

  /**
   * When the status check itself keeps failing, the outcome is unknown. The check must be retried
   * rather than giving up after a single attempt.
   */
  @Test
  public void testStatusCheckIsRetriedBeforeReportingUnknown() {
    GlueViewOperations ops = createView();
    ViewMetadata base = ops.current();

    ViewMetadata update =
        ViewMetadata.buildFrom(base)
            .setProperties(
                ImmutableMap.of(
                    TableProperties.COMMIT_NUM_STATUS_CHECKS,
                    "2",
                    TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS,
                    "1",
                    TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS,
                    "10",
                    TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS,
                    "10000"))
            .build();

    Mockito.doAnswer(
            invocation -> {
              // from here on, every commit status check against Glue fails
              failStatusChecks.set(true);
              throw new RuntimeException("Datacenter on fire");
            })
        .when(glue)
        .updateTable(Mockito.any(UpdateTableRequest.class));

    assertThatThrownBy(() -> ops.commit(base, update))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageContaining("Datacenter on fire");

    assertThat(statusCheckAttempts.get())
        .as("The commit status check should have been retried, not attempted only once")
        .isGreaterThan(1);

    String newMetadataLocation = attemptedMetadataLocation();
    assertThat(newMetadataLocation).isNotNull();
    assertThat(fileIO(ops).fileExists(newMetadataLocation))
        .as("Commit outcome is unknown, so the new metadata file must not be deleted")
        .isTrue();
  }

  @Test
  public void testLakeFormationCreateViewPublishesThroughAPlaceholderEntry() {
    lakeFormationCatalog()
        .buildView(VIEW_IDENTIFIER)
        .withSchema(SCHEMA)
        .withDefaultNamespace(Namespace.of(DATABASE_NAME))
        .withQuery("spark", "select id from db.tbl")
        .create();

    InOrder inOrder = Mockito.inOrder(glue);
    ArgumentCaptor<CreateTableRequest> placeholder =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    inOrder.verify(glue).createTable(placeholder.capture());
    ArgumentCaptor<UpdateTableRequest> published =
        ArgumentCaptor.forClass(UpdateTableRequest.class);
    inOrder.verify(glue).updateTable(published.capture());

    TableInput placeholderInput = placeholder.getValue().tableInput();
    assertThat(placeholder.getValue().catalogId()).isEqualTo(GLUE_CATALOG_ID);
    assertThat(placeholderInput.tableType()).isEqualTo("VIRTUAL_VIEW");
    assertThat(placeholderInput.storageDescriptor().location()).isNotBlank();
    assertThat(placeholderInput.parameters())
        .containsEntry(BaseMetastoreTableOperations.TABLE_TYPE_PROP, "iceberg-view")
        .doesNotContainKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);

    assertThat(published.getValue().catalogId()).isEqualTo(GLUE_CATALOG_ID);
    assertThat(published.getValue().tableInput().parameters())
        .containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
  }

  @Test
  public void testLakeFormationPlaceholderEntryIsRemovedWhenTheCommitFails() {
    Mockito.doThrow(
            AccessDeniedException.builder()
                .message("not authorized to perform: glue:UpdateTable")
                .build())
        .when(glue)
        .updateTable(Mockito.any(UpdateTableRequest.class));

    assertThatThrownBy(
            () ->
                lakeFormationCatalog()
                    .buildView(VIEW_IDENTIFIER)
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(Namespace.of(DATABASE_NAME))
                    .withQuery("spark", "select id from db.tbl")
                    .create())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("insufficient permissions");

    ArgumentCaptor<DeleteTableRequest> captor = ArgumentCaptor.forClass(DeleteTableRequest.class);
    Mockito.verify(glue).deleteTable(captor.capture());
    assertThat(captor.getValue().catalogId())
        .as("The placeholder has to be removed from the catalog it was created in")
        .isEqualTo(GLUE_CATALOG_ID);
    assertThat(captor.getValue().name()).isEqualTo(VIEW_NAME);
  }

  private GlueCatalog lakeFormationCatalog() {
    GlueCatalog catalog = new GlueCatalog();
    catalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(
            ImmutableMap.of(
                AwsProperties.GLUE_LAKEFORMATION_ENABLED,
                "true",
                AwsProperties.GLUE_CATALOG_ID,
                GLUE_CATALOG_ID)),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
    return catalog;
  }

  private GlueViewOperations createView() {
    glueCatalog
        .buildView(VIEW_IDENTIFIER)
        .withSchema(SCHEMA)
        .withDefaultNamespace(Namespace.of(DATABASE_NAME))
        .withQuery("spark", "select id from db.tbl")
        .create();

    return (GlueViewOperations) glueCatalog.newViewOps(VIEW_IDENTIFIER);
  }

  private static ViewMetadata updatedMetadata(ViewMetadata base) {
    return ViewMetadata.buildFrom(base).setProperties(ImmutableMap.of("key", "value")).build();
  }

  /** The metadata location that the last {@code updateTable} call attempted to publish. */
  private String attemptedMetadataLocation() {
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    Mockito.verify(glue, Mockito.atLeastOnce()).updateTable(captor.capture());
    Map<String, String> parameters = captor.getValue().tableInput().parameters();
    return parameters.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
  }

  private static InMemoryFileIO fileIO(GlueViewOperations ops) {
    return (InMemoryFileIO) ops.io();
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
