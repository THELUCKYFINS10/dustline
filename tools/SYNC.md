# Keeping the site in step with the artifact

The game lives at https://claude.ai/artifact/8hBCVPbdtXUGkGD9tiXfTD. This repo's root is a built copy of it.
To rebuild: put the artifact's files (index.html and every other published file) in a folder `art/`, then
`python3 tools/build.py art out`, copy `out/*` over the repo root, and write the artifact's version id to
`tools/artifact-version.txt`. build.py adds Firebase + claude-shim.js (from tools/src) to the page.
