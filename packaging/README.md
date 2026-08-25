# Packaging & distribution channels

What exists, what each channel needs, and the publication steps. The audit's
finding stands until these are published: **a driver competing with
vendor-bundled installers lives or dies on `brew install` / `winget install`
/ `apt install` working.** Everything below is release-blocked — cut the
first tag, then publish in this order.

| Channel | Artifact here | Publication step |
|---|---|---|
| GitHub Releases | `release.yml` builds tar.gz/deb/rpm/pkg/exe on a `v*` tag | `git tag v0.6.0 && git push --tags` |
| Homebrew (macOS/Linux) | `homebrew/argus-odbc.rb` | create the `homebrew-argus` tap repo, fill url+sha256, copy to `Formula/` |
| winget (Windows) | `winget/VargaFoundation.ArgusODBC.yaml` | `wingetcreate new <installer-url>` → PR to microsoft/winget-pkgs |
| apt repository | `build-deb.sh` output | serve a signed repo from gh-pages (`apt-ftparchive` or aptly); document the sources.list line |
| yum/dnf repository | `argus-odbc.spec` output | `createrepo_c` + gh-pages; sign with the release GPG key |
| Chocolatey | — | optional once winget is live (choco pulls the same NSIS exe) |
| Intune / MDM | `installer/intune/` + `.intunewin` in releases | already wired |

## Notes

- **One version, everywhere**: every channel's version string comes from the
  git tag; nothing is hand-edited per channel.
- The deb/rpm scripts compute real shared-library dependencies
  (`dpkg-shlibdeps`); keep it that way when adding backends.
- Linux artifacts must gain a detached GPG signature when the apt/yum repos
  go live — a repo without signing is worse than none.
- ARM64: macOS arm64 comes free via Homebrew on Apple Silicon; add a
  `ubuntu-24.04-arm` build job when the first Linux arm64 request lands.
