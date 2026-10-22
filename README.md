# Funkenpfad (gebaut)

Dieses Repository enthält nur das **fertig gebaute** Spiel Funkenpfad, kein Quelltext. Es wird
bei jedem Push auf `master` des privaten Quell-Repositorys automatisch neu geschrieben
(Workflow „Veröffentlichen“). Bitte hier nichts von Hand ändern.

| Zweig | Inhalt | Wofür |
|---|---|---|
| `master` | `server.mjs` (Online-Server, eine Datei ohne Abhängigkeiten), `web/` (das Spiel), `funkenpfad.caddy` | eigener Server: `/srv/funkenpfad`, ausrollen mit `deploy-app funkenpfad` |
| `gh-pages` | nur das Spiel | optional GitHub Pages |

Der Server liest seine Einstellungen aus `.dev.vars` neben `server.mjs` (nicht im Repository):
`PORT`, `ALLOWED_ORIGINS`, `TRUST_PROXY`, `DATA_DIR`. Gesundheit: `curl http://127.0.0.1:8010/health`.
