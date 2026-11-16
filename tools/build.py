#!/usr/bin/env python3
"""Turn the Dustline artifact's published files into the website.

usage: build.py <artifact_dir> <out_dir>

<artifact_dir> holds the artifact's files exactly as claude.ai serves them (index.html, music, models...).
The game runs unchanged: this only adds, at the top of <head>, the Firebase SDK, the site's Firebase config and
claude-shim.js, which gives the game the same claude.use('user'|'db'|'room') tools it has on claude.ai.
"""
import hashlib, json, os, re, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, 'src')

def main(art, out):
    page = os.path.join(art, 'index.html')
    html = open(page, encoding='utf-8').read()
    if 'claude-shim.js' in html:
        sys.exit('index.html already has the shim; pass the artifact files, not a built site')
    h = hashlib.sha256(html.encode('utf-8'))
    for f in ('claude-shim.js', 'config.js', 'rest-db.js'):
        h.update(open(os.path.join(SRC, f), 'rb').read())
    build = h.hexdigest()[:12]
    # the build id lets open pages notice a newer site and refresh themselves (see claude-shim.js)
    inject = ('<script>window.DUSTLINE_BUILD="%s";</script>'
              '<script src="config.js?v=%s"></script>'
              '<script src="vendor/firebase-app-compat.js"></script>'
              '<script src="vendor/firebase-auth-compat.js"></script>'
              '<script src="rest-db.js?v=%s"></script>'
              '<script src="claude-shim.js?v=%s"></script>') % (build, build, build, build)
    m = re.search(r'<head[^>]*>', html, re.I)
    if not m:
        sys.exit('no <head> in index.html')
    html = html[:m.end()] + inject + html[m.end():]
    # the game's tab title on the website
    if '<title>' not in html[:m.end() + len(inject) + 2000]:
        html = html[:m.end()] + '<title>Dustline</title>' + html[m.end():]
    os.makedirs(out, exist_ok=True)
    # artifact files first, then the site's own files on top
    for name in os.listdir(art):
        p = os.path.join(art, name)
        if os.path.isfile(p) and name != 'index.html':
            shutil.copy2(p, os.path.join(out, name))
    open(os.path.join(out, 'index.html'), 'w', encoding='utf-8').write(html)
    shutil.copytree(os.path.join(SRC, 'vendor'), os.path.join(out, 'vendor'), dirs_exist_ok=True)
    shutil.copy2(os.path.join(SRC, 'claude-shim.js'), os.path.join(out, 'claude-shim.js'))
    shutil.copy2(os.path.join(SRC, 'rest-db.js'), os.path.join(out, 'rest-db.js'))
    shutil.copy2(os.path.join(SRC, 'config.js'), os.path.join(out, 'config.js'))
    open(os.path.join(out, '.nojekyll'), 'w').close()
    json.dump({'build': build}, open(os.path.join(out, 'version.json'), 'w'))
    print('built', out, 'from', art, 'build', build)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
