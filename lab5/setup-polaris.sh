#!/usr/bin/env bash
# Configure Polaris: create catalog, set up RBAC roles, assign to root.
# Run after `docker compose up` once Polaris is healthy.
set -euo pipefail

POLARIS=http://localhost:8181

echo "==> Fetching access token..."
ACCESS_TOKEN=$(curl -s -X POST \
  "$POLARIS/api/catalog/v1/oauth/tokens" \
  -d 'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')

if [[ -z "$ACCESS_TOKEN" || "$ACCESS_TOKEN" == "null" ]]; then
  echo "ERROR: Could not obtain access token. Is Polaris running at $POLARIS?"
  exit 1
fi
echo "Access token obtained."

echo ""
echo "==> Creating 'polariscatalog' catalog (backed by MinIO)..."
curl -si -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  "$POLARIS/api/management/v1/catalogs" \
  --data '{
    "name": "polariscatalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse",
      "s3.endpoint": "http://minio:9000",
      "s3.path-style-access": "true",
      "s3.access-key-id": "admin",
      "s3.secret-access-key": "password",
      "s3.region": "dummy-region"
    },
    "storageConfigInfo": {
      "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
      "storageType": "S3",
      "allowedLocations": ["s3://warehouse/*"]
    }
  }' || true

echo ""
echo "==> Granting CATALOG_MANAGE_CONTENT to catalog_admin role..."
curl -s -X PUT \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  "$POLARIS/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants" \
  --data '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' \
  | jq .

echo ""
echo "==> Creating 'data_engineer' principal role..."
curl -s -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  "$POLARIS/api/management/v1/principal-roles" \
  --data '{"principalRole":{"name":"data_engineer"}}' \
  | jq . || true

echo ""
echo "==> Assigning catalog_admin → data_engineer..."
curl -s -X PUT \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  "$POLARIS/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog" \
  --data '{"catalogRole":{"name":"catalog_admin"}}' \
  | jq .

echo ""
echo "==> Assigning data_engineer → root principal..."
curl -s -X PUT \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  "$POLARIS/api/management/v1/principals/root/principal-roles" \
  --data '{"principalRole":{"name":"data_engineer"}}' \
  | jq .

echo ""
echo "==> Verifying catalog..."
curl -s -X GET \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  "$POLARIS/api/management/v1/catalogs" | jq .

echo ""
echo "==> Verifying root roles..."
curl -s -X GET \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  "$POLARIS/api/management/v1/principals/root/principal-roles" | jq .

echo ""
echo "Setup complete. Connect to Trino:"
echo "  docker compose exec -it trino trino --server localhost:8080 --catalog iceberg"
echo "  (Trino Web UI: http://localhost:8090)"
