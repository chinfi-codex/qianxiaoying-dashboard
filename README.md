# 赚钱效应 Dashboard (GitHub Pages)

A lightweight, public, static dashboard for daily “赚钱效应” analysis.

## Structure

- `site/` - static website (HTML/CSS/JS)
- `data/` - generated JSON snapshots

## Deploy

GitHub Actions deploys `site/` to the `gh-pages` branch.

## Next

- Add daily generator that pulls Tushare data and writes `data/YYYY-MM-DD.json` + `data/latest.json`.
