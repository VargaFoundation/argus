# Packaging & distribution channels

What exists, what each channel needs, and the publication steps. The audit's
finding stands until these are published: **a driver competing with
vendor-bundled installers lives or dies on `brew install` / `winget install`
/ `apt install` working.** Everything below is release-blocked — cut the
first tag, then publish in this order.

| Channel | Artifact here | Publication step |
|---|---|---|
| GitHub Releases | `release.yml` builds tar.gz/deb/rpm/pkg/exe on a `v*` tag | `git tag v0.6.0 && git push --tags` |
| Homebrew (macOS/Linux) | `homebrew/argus-odbc.rb` — url+sha256 filled for v0.6.0 | create the `homebrew-argus` tap repo, copy to `Formula/` |
| winget (Windows) | `winget/VargaFoundation.ArgusODBC.yaml` — url+sha256 filled for v0.6.0 | `wingetcreate new <installer-url>` → PR to microsoft/winget-pkgs |
| apt repository | `build-deb.sh` output | serve a signed repo from gh-pages (`apt-ftparchive` or aptly); document the sources.list line |
| yum/dnf repository | `argus-odbc.spec` output | `createrepo_c` + gh-pages; sign with the release GPG key |
| Chocolatey | — | optional once winget is live (choco pulls the same NSIS exe) |
| Intune / MDM | `installer/intune/` + `.intunewin` in releases | already wired |

## Notes

- **One version, everywhere**: every channel's version string comes from the
  git tag; nothing is hand-edited per channel. The two manifests are the
  exception -- they pin a url and a hash, so both need bumping on release.
  The command to recompute each hash is in a comment at the top of the file.
- The winget manifest's `nullsoft` type and `/S` silent switch are not
  guesses: the v0.6.0 installer was run silently on a real Windows 11 machine
  and returned exit code 0.
- The deb/rpm scripts compute real shared-library dependencies
  (`dpkg-shlibdeps`); keep it that way when adding backends.
- Linux artifacts are GPG-signed by the release workflow: an embedded header
  signature in the .rpm (`rpm -K`), a detached `.asc` beside the .deb and the
  tarball, and a signed `SHA256SUMS` covering every published file. The public
  key is in `KEYS` at the repo root. The apt/yum repos, when they go live,
  reuse that same key to sign their metadata.
- Signing is gated on the `GPG_PRIVATE_KEY` secret and every step is
  `continue-on-error`, matching the Windows and macOS signing: a missing or
  expired key degrades to unsigned packages instead of blocking a release.
- ARM64: macOS arm64 comes free via Homebrew on Apple Silicon; add a
  `ubuntu-24.04-arm` build job when the first Linux arm64 request lands.
