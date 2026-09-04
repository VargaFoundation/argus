/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ARGUS_BROWSER_H
#define ARGUS_BROWSER_H

/*
 * browser.h - Hand a URL to the user's web browser without a shell.
 *
 * Used by the interactive OAuth2 authorization-code flow. The URL is built
 * from DSN / connection-string material (the authorization endpoint), which
 * a shared DSN, an .odc or a .tds file can carry, so it must never reach
 * system(): the launcher is exec'ed with an argv (POSIX) or handed to
 * ShellExecute (Windows), and the URL is validated first.
 */

#include <stdbool.h>
#include <stddef.h>

/*
 * True when `url` may be handed to a browser: an https:// URL, or an http://
 * URL whose host is the local machine (a development IdP), made only of
 * printable ASCII characters that can appear in a URL. Quotes, whitespace,
 * control and non-ASCII bytes are refused.
 */
bool argus_browser_url_ok(const char *url);

/*
 * Start the user's browser on `url`. Honours $BROWSER (a program name,
 * optionally with arguments and a `%s` placeholder for the URL; split like
 * a shell would, never run by one), then xdg-open, then open (macOS). On
 * Windows the URL is handed to ShellExecute for its registered handler.
 * The launcher is not waited for. Returns 0 once a launcher has started,
 * -1 otherwise with `errmsg` filled in. A URL argus_browser_url_ok()
 * rejects is refused without starting anything.
 */
int argus_browser_open(const char *url, char *errmsg, size_t errmsg_size);

#endif /* ARGUS_BROWSER_H */
