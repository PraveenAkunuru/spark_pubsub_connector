# Restart Guide: Cloud Verification (Dataproc 2.3)

## Current Context
- **Cluster**: `cluster-aaf3` (Master: `cluster-aaf3-m`)
- **Environment**: Dataproc 2.3 (Debian 12, Spark 3.5.3, Java 11)
- **Problem**: `init()` returning 0 on driver and executors.
- **Errors Identified**:
    1. `ConnectError(Custom { kind: InvalidData, error: InvalidCertificate(UnknownIssuer) })` -> Should be fixed by explicit `/etc/ssl/certs/ca-certificates.crt` loading.
    2. `ACCESS_TOKEN_TYPE_UNSUPPORTED` -> Currently being addressed by adding explicit scopes (`cloud-platform`, `pubsub`).

## Work Since Last Checkpoint
- **Native Fixes**:
    - Added explicit CA loading from `/etc/ssl/certs/ca-certificates.crt`.
    - Added explicit auth scopes in `pubsub.rs`.
    - Added logging for token length and prefix.
- **Diagnostic Fixes**:
    - Modified `ExecutorDiag.scala` to write a check file to `/tmp/executor_diag_disk_check.txt`.
    - Automated cleanup of old jobs before submitting new ones.

## Ongoing Command
- A build/deploy/submit command was running. After restart, check for a new job ID.
- `gcloud dataproc jobs list --region us-central1 --project pakunuru-1119-20250930202256 --limit 5`

## Alternative Root Cause Theories (if auth fail persists)
1. **Clock Skew**: Check if the Dataproc node clock is accurate.
2. **Metadata API**: The `google-cloud-auth` crate might be using an older metadata API version that Dataproc restricts for certain scopes.
3. **Scope vs IAM**: Even if IAM is correct, some tokens aren't "privileged" enough without the specific `cloud-platform` scope.
4. **Project ID Mismatch**: Verify if using Project Number instead of ID changes token acceptance.
5. **Token Prefix**: Check if the token returned by the metadata server has a prefix that `tonic` shouldn't include (e.g. "Bearer" being added twice).

## Next Steps
1. Check the output of the job submitted just before restart.
2. Verify if `/tmp/executor_diag_disk_check.txt` exists on the master node (via SSH).
3. If `init()` still fails, look at the token debug logs (length/prefix).

```bash
# SSH into master to check local logs and disk file
gcloud compute ssh --zone "us-central1-c" "cluster-aaf3-m" --tunnel-through-iap --project "pakunuru-1119-20250930202256"
# In SSH:
cat /tmp/executor_diag_disk_check.txt
```
