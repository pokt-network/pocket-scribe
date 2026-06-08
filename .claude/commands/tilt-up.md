---
description: Bring up the local dev stack with Tilt + health checks. Verifies cluster, namespaces, secrets.
---

Bring up the PocketScribe local development environment.

Steps:
1. Check `kind get clusters` for an existing `pocketscribe-dev` cluster. If not, run `make cluster-up`.
2. Check `kubectl get ns pocketscribe-dev` exists; create if not.
3. Verify required local files exist:
   - `Tiltfile`
   - `configs/dev/poktroll-app.toml`
   - `deploy/docker/Dockerfile.dev`
4. Run `tilt up` in foreground.
5. Open the Tilt UI: print URL `http://localhost:10350`.
6. Once all resources are green in Tilt, run `ps doctor` to confirm health.

Report:
```
✅ Cluster ready: kind pocketscribe-dev
✅ Namespace: pocketscribe-dev
✅ Tilt running: http://localhost:10350
✅ ps doctor: all green

Stack ready for development.
```

If any step fails, report the failure and suggest the fix.
