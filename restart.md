# Restart Guide: Cloud Verification (Dataproc 2.3)

## Current Context
- **Cluster**: `cluster-aaf3` (Master: `cluster-aaf3-m`)
- **Environment**: Dataproc 2.3 (Debian 12, Spark 3.5.3, Java 11)
- **Active Job**: `9bc1299827e34c45891b7c75c8512890` (Running)
- **Git Status**: All changes pushed to `main`.
- **Debug Summary**: See `CLOUD_DEBUG_SUMMARY.md` for full details.

## Work Completed
1. **Fixed TLS**: Loaded `/etc/ssl/certs/ca-certificates.crt` explicitly.
2. **Fixed Auth**: Added explicit `cloud-platform` and `pubsub` scopes.
3. **Improved Diagnostics**: Added raw `println!` to capture `get_subscription` errors.
4. **Pushed Code**: Remote repo is up to date.

## Next Steps Upon Return
1. **Check Job Logs**:
   ```bash
   gcloud dataproc jobs describe 9bc1299827e34c45891b7c75c8512890 --region us-central1 --project pakunuru-1119-20250930202256 --format="value(driverOutputResourceUri)"
   # Then cat the output looking for "Rust Raw:"
   ```
2. **Analyze Error**: You are looking for why `get_subscription` failed (e.g., `PERMISSION_DENIED`, `NOT_FOUND`).
3. **Review**: `CLOUD_DEBUG_SUMMARY.md` is ready for review.
