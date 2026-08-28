# Diff Details

Date : 2026-08-28 09:16:28

Directory /home/g/git/slipway/web

Total : 78 files,  -13561 codes, -505 comments, -1285 blanks, all -15351 lines

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [internal/cli/check.go](/internal/cli/check.go) | Go | -102 | 0 | -11 | -113 |
| [internal/cli/check\_test.go](/internal/cli/check_test.go) | Go | -282 | 0 | -24 | -306 |
| [internal/cli/cli.go](/internal/cli/cli.go) | Go | -566 | -5 | -46 | -617 |
| [internal/cli/cli\_test.go](/internal/cli/cli_test.go) | Go | -231 | 0 | -17 | -248 |
| [internal/cli/daemon.go](/internal/cli/daemon.go) | Go | -178 | -7 | -18 | -203 |
| [internal/cli/managed.go](/internal/cli/managed.go) | Go | -376 | -2 | -33 | -411 |
| [internal/cli/managed\_test.go](/internal/cli/managed_test.go) | Go | -747 | 0 | -47 | -794 |
| [internal/config/config.go](/internal/config/config.go) | Go | -431 | -40 | -39 | -510 |
| [internal/config/config\_test.go](/internal/config/config_test.go) | Go | -688 | 0 | -25 | -713 |
| [internal/config/discovery.go](/internal/config/discovery.go) | Go | -148 | -6 | -19 | -173 |
| [internal/config/discovery\_test.go](/internal/config/discovery_test.go) | Go | -188 | 0 | -22 | -210 |
| [internal/config/fingerprint.go](/internal/config/fingerprint.go) | Go | -19 | -4 | -4 | -27 |
| [internal/config/fingerprint\_test.go](/internal/config/fingerprint_test.go) | Go | -180 | 0 | -17 | -197 |
| [internal/config/paths.go](/internal/config/paths.go) | Go | -259 | -21 | -22 | -302 |
| [internal/config/paths\_test.go](/internal/config/paths_test.go) | Go | -388 | 0 | -28 | -416 |
| [internal/control/client.go](/internal/control/client.go) | Go | -326 | -21 | -27 | -374 |
| [internal/control/client\_test.go](/internal/control/client_test.go) | Go | -133 | 0 | -19 | -152 |
| [internal/control/logs.go](/internal/control/logs.go) | Go | -248 | -19 | -23 | -290 |
| [internal/control/logs\_test.go](/internal/control/logs_test.go) | Go | -127 | 0 | -9 | -136 |
| [internal/control/manager.go](/internal/control/manager.go) | Go | -1,086 | -60 | -71 | -1,217 |
| [internal/control/manager\_test.go](/internal/control/manager_test.go) | Go | -1,195 | 0 | -68 | -1,263 |
| [internal/control/server.go](/internal/control/server.go) | Go | -573 | -31 | -51 | -655 |
| [internal/control/server\_test.go](/internal/control/server_test.go) | Go | -16 | 0 | -3 | -19 |
| [internal/control/transport\_test.go](/internal/control/transport_test.go) | Go | -514 | -2 | -42 | -558 |
| [internal/control/types.go](/internal/control/types.go) | Go | -65 | -34 | -14 | -113 |
| [internal/daemon/daemon.go](/internal/daemon/daemon.go) | Go | -94 | -5 | -12 | -111 |
| [internal/daemon/daemon\_test.go](/internal/daemon/daemon_test.go) | Go | -141 | 0 | -10 | -151 |
| [internal/daemon/many.go](/internal/daemon/many.go) | Go | -139 | -13 | -13 | -165 |
| [internal/daemon/many\_test.go](/internal/daemon/many_test.go) | Go | -252 | 0 | -27 | -279 |
| [internal/executor/executor.go](/internal/executor/executor.go) | Go | -232 | -38 | -30 | -300 |
| [internal/executor/executor\_test.go](/internal/executor/executor_test.go) | Go | -246 | -1 | -20 | -267 |
| [internal/executor/process\_other.go](/internal/executor/process_other.go) | Go | -4 | -4 | -5 | -13 |
| [internal/executor/process\_unix.go](/internal/executor/process_unix.go) | Go | -30 | -7 | -5 | -42 |
| [internal/executor/process\_unix\_test.go](/internal/executor/process_unix_test.go) | Go | -168 | -4 | -15 | -187 |
| [internal/queue/commands.go](/internal/queue/commands.go) | Go | -98 | -2 | -6 | -106 |
| [internal/queue/history.go](/internal/queue/history.go) | Go | -333 | -12 | -25 | -370 |
| [internal/queue/jobs.go](/internal/queue/jobs.go) | Go | -309 | -12 | -22 | -343 |
| [internal/queue/store.go](/internal/queue/store.go) | Go | -240 | -22 | -38 | -300 |
| [internal/queue/store\_test.go](/internal/queue/store_test.go) | Go | -610 | -4 | -46 | -660 |
| [internal/queue/types.go](/internal/queue/types.go) | Go | -127 | -21 | -18 | -166 |
| [internal/watcher/backend\_mode\_darwin.go](/internal/watcher/backend_mode_darwin.go) | Go | -2 | -4 | -3 | -9 |
| [internal/watcher/backend\_mode\_darwin\_test.go](/internal/watcher/backend_mode_darwin_test.go) | Go | -19 | -1 | -5 | -25 |
| [internal/watcher/backend\_mode\_linux\_test.go](/internal/watcher/backend_mode_linux_test.go) | Go | -16 | -1 | -5 | -22 |
| [internal/watcher/backend\_mode\_other.go](/internal/watcher/backend_mode_other.go) | Go | -2 | -1 | -3 | -6 |
| [internal/watcher/match.go](/internal/watcher/match.go) | Go | -71 | -5 | -7 | -83 |
| [internal/watcher/match\_test.go](/internal/watcher/match_test.go) | Go | -71 | 0 | -5 | -76 |
| [internal/watcher/polling.go](/internal/watcher/polling.go) | Go | -121 | -3 | -9 | -133 |
| [internal/watcher/polling\_test.go](/internal/watcher/polling_test.go) | Go | -296 | 0 | -26 | -322 |
| [internal/watcher/root\_identity.go](/internal/watcher/root_identity.go) | Go | -68 | -14 | -8 | -90 |
| [internal/watcher/root\_identity\_test.go](/internal/watcher/root_identity_test.go) | Go | -215 | 0 | -19 | -234 |
| [internal/watcher/stable.go](/internal/watcher/stable.go) | Go | -125 | -18 | -13 | -156 |
| [internal/watcher/stable\_test.go](/internal/watcher/stable_test.go) | Go | -107 | 0 | -12 | -119 |
| [internal/watcher/watcher.go](/internal/watcher/watcher.go) | Go | -887 | -28 | -57 | -972 |
| [internal/watcher/watcher\_test.go](/internal/watcher/watcher_test.go) | Go | -824 | -2 | -58 | -884 |
| [internal/webui/api.go](/internal/webui/api.go) | Go | -588 | 0 | -49 | -637 |
| [internal/webui/api\_test.go](/internal/webui/api_test.go) | Go | -312 | 0 | -24 | -336 |
| [internal/webui/dist/assets/index-Ch0TT8xm.css](/internal/webui/dist/assets/index-Ch0TT8xm.css) | PostCSS | -1 | 0 | -1 | -2 |
| [internal/webui/dist/assets/index-DG\_S3TIe.js](/internal/webui/dist/assets/index-DG_S3TIe.js) | JavaScript | -9 | 0 | -1 | -10 |
| [internal/webui/dist/index.html](/internal/webui/dist/index.html) | HTML | -17 | 0 | -1 | -18 |
| [internal/webui/dist/theme-init.js](/internal/webui/dist/theme-init.js) | JavaScript | -24 | -2 | -5 | -31 |
| [internal/webui/server.go](/internal/webui/server.go) | Go | -309 | -13 | -27 | -349 |
| [internal/webui/server\_test.go](/internal/webui/server_test.go) | Go | -119 | 0 | -6 | -125 |
| [internal/worker/worker.go](/internal/worker/worker.go) | Go | -372 | -20 | -39 | -431 |
| [internal/worker/worker\_test.go](/internal/worker/worker_test.go) | Go | -525 | 0 | -44 | -569 |
| [web/index.html](/web/index.html) | HTML | 16 | 0 | 1 | 17 |
| [web/package-lock.json](/web/package-lock.json) | JSON | 1,841 | 0 | 1 | 1,842 |
| [web/package.json](/web/package.json) | JSON | 22 | 0 | 1 | 23 |
| [web/public/theme-init.js](/web/public/theme-init.js) | JavaScript | 24 | 2 | 5 | 31 |
| [web/src/App.tsx](/web/src/App.tsx) | TypeScript JSX | 977 | 0 | 71 | 1,048 |
| [web/src/api.ts](/web/src/api.ts) | TypeScript | 59 | 1 | 6 | 66 |
| [web/src/main.tsx](/web/src/main.tsx) | TypeScript JSX | 9 | 0 | 2 | 11 |
| [web/src/styles.css](/web/src/styles.css) | PostCSS | 493 | 0 | 19 | 512 |
| [web/src/theme.ts](/web/src/theme.ts) | TypeScript | 36 | 1 | 9 | 46 |
| [web/src/types.ts](/web/src/types.ts) | TypeScript | 104 | 0 | 13 | 117 |
| [web/tsconfig.app.json](/web/tsconfig.app.json) | JSON | 20 | 0 | 1 | 21 |
| [web/tsconfig.json](/web/tsconfig.json) | JSON with Comments | 7 | 0 | 1 | 8 |
| [web/tsconfig.node.json](/web/tsconfig.node.json) | JSON | 11 | 0 | 1 | 12 |
| [web/vite.config.ts](/web/vite.config.ts) | TypeScript | 9 | 0 | 2 | 11 |

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details