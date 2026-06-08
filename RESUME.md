# Resume message — paste at start of next session

```
Continuando trabajo en PocketScribe. Lee HANDOFF.md para el contexto completo.

Tres cosas críticas al retomar:

1. ⚠️ ORCHESTRATOR ARQUEOLOGY DEAD desde 07:23 hoy con FATAL en v0.1.30
   (después de 60 retries con MAX_RETRIES bumped). Avance solo h=595475 →
   h=599435 (~4K blocks en 3h, ~60 blocks/retry). Bucket tiene v0.1.28 +
   v0.1.29 OK. NO relanzar a ciegas — necesita debug del stall pattern
   primero (ver hipótesis 1-4 en HANDOFF.md).

2. ✅ REPO PÚBLICO YA INICIALIZADO en git@github.com:pokt-network/pocket-scribe.git
   con Initial commit 150fcd1 (32 archivos LFS, 5.3 GB). Foundation lista
   para arrancar Phase 1.

3. PRÓXIMO PASO planeado: Phase 1 spike — wiring del primer consumer Go
   (supplier) + decoder runtime + 1 aggregate (claims_hourly) + Hasura/
   PostgREST sobre el schema de 244 tablas que ya está validado.

¿Por dónde arrancamos: debug del orchestrator dead o Phase 1 spike?
```

## (For myself — context to load at session start)

Read in this order:
1. `HANDOFF.md` — what happened in the prior session
2. `STATUS.md` — what exists today
3. `docs/architecture/00-system-flow.md` — visual overview
4. `docs/decisions/ADR-028-schema-versioning-strategy.md` — the schema design
5. `ROADMAP.md` — Phase 1 spike scope

For orchestrator debug specifically:
- `archeology/scripts/orchestrator.sh` — the script (has tip-mode + retry logic)
- Run `ssh.exe pnf@65.108.199.125 'tail -50 /mnt/scribe/work/logs/orchestrator.log'`
  to see current state
- Run `ssh.exe pnf@65.108.199.125 'tmux ls'` to check session liveness

For Phase 1 spike:
- Schema is locked → start writing Go (per [ROADMAP.md](./ROADMAP.md) Phase 1)
- First decoder + first consumer + first aggregate
- Resurrect `internal/*` dirs deliberately as we build them
