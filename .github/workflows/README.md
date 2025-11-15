# GitHub Actions Workflows

## Setup Required

To enable automated deployments, you need to configure Azure credentials as a GitHub secret.

### One-Time Setup

1. **Create Azure Service Principal:**

```bash
az ad sp create-for-rbac \
  --name "github-actions-lm-ingest" \
  --role contributor \
  --scopes /subscriptions/{YOUR_SUBSCRIPTION_ID}/resourceGroups/CTA_Resource_Group \
  --sdk-auth
```

2. **Copy the JSON output** (it will look like this):

```json
{
  "clientId": "<GUID>",
  "clientSecret": "<STRING>",
  "subscriptionId": "<GUID>",
  "tenantId": "<GUID>",
  ...
}
```

3. **Add to GitHub Secrets:**
   - Go to GitHub repository → Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `AZURE_CREDENTIALS`
   - Value: Paste the entire JSON output
   - Click "Add secret"

### Verify Setup

After adding the secret, the GitHub Action will automatically run on:
- Push to `feature/production-redesign` branch
- Push to `main` branch
- Manual trigger from Actions tab

### Manual Trigger

1. Go to Actions tab in GitHub
2. Select "Deploy to Azure Container App"
3. Click "Run workflow"
4. Enter version tag (e.g., v11)
5. Click "Run workflow"

---

**Note:** The service principal needs `contributor` role on the resource group to:
- Build Docker images in ACR
- Update Container App
- Get access tokens for PostgreSQL

**Security:** GitHub encrypts secrets and never exposes them in logs (marked with `::add-mask::`).
