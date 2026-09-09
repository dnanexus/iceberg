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
package org.apache.iceberg.aws.lakeformation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory.LakeFormationCredentialsProvider;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.PermissionType;

class TestLakeFormationCredentialsProvider {

  private static final String TABLE_ARN = "arn:aws:glue:us-east-1:123456789012:table/db/table";

  @Test
  void cachesUntilExpiration() {
    LakeFormationClient client = mock(LakeFormationClient.class);
    Instant expiresAt = Instant.now().plus(1, ChronoUnit.HOURS);
    when(client.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(credentialsResponse("key-1", "secret-1", "token-1", expiresAt));

    try (LakeFormationCredentialsProvider provider =
        new LakeFormationCredentialsProvider(client, TABLE_ARN)) {
      AwsCredentials first = provider.resolveCredentials();
      for (int i = 0; i < 5; i++) {
        assertThat(provider.resolveCredentials()).isSameAs(first);
      }

      assertSessionCredentials(first, "key-1", "secret-1", "token-1", expiresAt);
    }

    verify(client, times(1))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }

  @Test
  void refreshesExpiredCredentials() {
    LakeFormationClient client = mock(LakeFormationClient.class);
    Instant expiredAt = Instant.now().minus(1, ChronoUnit.MINUTES);
    Instant refreshedExpiresAt = Instant.now().plus(1, ChronoUnit.HOURS);
    when(client.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(credentialsResponse("key-1", "secret-1", "token-1", expiredAt))
        .thenReturn(credentialsResponse("key-2", "secret-2", "token-2", refreshedExpiresAt));

    try (LakeFormationCredentialsProvider provider =
        new LakeFormationCredentialsProvider(client, TABLE_ARN)) {
      AwsCredentials first = provider.resolveCredentials();
      assertSessionCredentials(first, "key-1", "secret-1", "token-1", expiredAt);

      AwsCredentials refreshed = provider.resolveCredentials();
      assertThat(refreshed).isNotSameAs(first);
      assertSessionCredentials(refreshed, "key-2", "secret-2", "token-2", refreshedExpiresAt);
    }

    verify(client, times(2))
        .getTemporaryGlueTableCredentials(any(GetTemporaryGlueTableCredentialsRequest.class));
  }

  @Test
  void missingExpirationFails() {
    LakeFormationClient client = mock(LakeFormationClient.class);
    when(client.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(credentialsResponse("key-1", "secret-1", "token-1", null));

    try (LakeFormationCredentialsProvider provider =
        new LakeFormationCredentialsProvider(client, TABLE_ARN)) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid Lake Formation credentials: expiration not set");
    }
  }

  @Test
  void requestsColumnPermissionCredentials() {
    LakeFormationClient client = mock(LakeFormationClient.class);
    Instant expiresAt = Instant.now().plus(1, ChronoUnit.HOURS);
    when(client.getTemporaryGlueTableCredentials(
            any(GetTemporaryGlueTableCredentialsRequest.class)))
        .thenReturn(credentialsResponse("key-1", "secret-1", "token-1", expiresAt));

    try (LakeFormationCredentialsProvider provider =
        new LakeFormationCredentialsProvider(client, TABLE_ARN)) {
      provider.resolveCredentials();
    }

    verify(client)
        .getTemporaryGlueTableCredentials(
            GetTemporaryGlueTableCredentialsRequest.builder()
                .tableArn(TABLE_ARN)
                .supportedPermissionTypes(PermissionType.COLUMN_PERMISSION)
                .build());
  }

  private static GetTemporaryGlueTableCredentialsResponse credentialsResponse(
      String accessKeyId, String secretAccessKey, String sessionToken, Instant expiration) {
    return GetTemporaryGlueTableCredentialsResponse.builder()
        .accessKeyId(accessKeyId)
        .secretAccessKey(secretAccessKey)
        .sessionToken(sessionToken)
        .expiration(expiration)
        .build();
  }

  private static void assertSessionCredentials(
      AwsCredentials credentials,
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      Instant expiration) {
    assertThat(credentials).isInstanceOf(AwsSessionCredentials.class);
    AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;
    assertThat(sessionCredentials.accessKeyId()).isEqualTo(accessKeyId);
    assertThat(sessionCredentials.secretAccessKey()).isEqualTo(secretAccessKey);
    assertThat(sessionCredentials.sessionToken()).isEqualTo(sessionToken);
    assertThat(sessionCredentials.expirationTime()).contains(expiration);
  }
}
