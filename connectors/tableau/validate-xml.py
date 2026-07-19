#!/usr/bin/env python3
"""
Validate the Argus Tableau connectors against Tableau's published XSDs.

Tableau's own connector-packager --validate-only does this, but it needs the
whole SDK checked out and installed. This does the schema half of the job with
nothing but the XSDs (downloaded once, then cached), which is what catches an
invented element or attribute early — the failure mode that otherwise only
shows up as a silent non-loading .taco on a machine with Tableau Desktop.

Passing here does NOT mean the connector works: only Tableau Desktop and TDVT
can say that. See README.md.

Usage:
    pip install xmlschema
    python connectors/tableau/validate-xml.py [connector-dir ...]

With no arguments, validates every connector directory next to this script.
"""
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

SDK_VALIDATION_URL = ("https://raw.githubusercontent.com/tableau/"
                      "connector-plugin-sdk/master/validation/")
CACHE_DIR = Path(__file__).resolve().parent / ".xsd-cache"

# Connector file -> the SDK schema that governs it.
FILE_SCHEMAS = [
    ("manifest.xml",           "connector_plugin_manifest_latest.xsd"),
    ("connectionFields.xml",   "connection_fields.xsd"),
    ("connectionMetadata.xml", "connector_plugin_metadata.xsd"),
    ("connectionResolver.tdr", "tdr_latest.xsd"),
    ("dialect.tdd",            "tdd_latest.xsd"),
]


def fetch_schema(name: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    dest = CACHE_DIR / name
    if not dest.exists():
        urllib.request.urlretrieve(SDK_VALIDATION_URL + name, dest)
    return dest


def validate_connector(plugin_dir: Path, xmlschema) -> int:
    print(f"── {plugin_dir.name}")
    failures = 0

    for xml_name, xsd_name in FILE_SCHEMAS:
        xml_path = plugin_dir / xml_name
        if not xml_path.exists():
            print(f"   MISSING   {xml_name}")
            failures += 1
            continue

        try:
            ET.parse(xml_path)
        except ET.ParseError as exc:
            print(f"   MALFORMED {xml_name}: {exc}")
            failures += 1
            continue

        try:
            schema = xmlschema.XMLSchema(str(fetch_schema(xsd_name)))
            schema.validate(str(xml_path))
        except Exception as exc:
            print(f"   INVALID   {xml_name}  (vs {xsd_name})")
            for line in str(exc).strip().splitlines()[:12]:
                print(f"             {line}")
            failures += 1
            continue

        print(f"   ok        {xml_name}")

    # connectionBuilder.js is referenced by the .tdr but not schema-checked;
    # its absence would only surface when Tableau tried to connect.
    if not (plugin_dir / "connectionBuilder.js").exists():
        print("   MISSING   connectionBuilder.js")
        failures += 1

    return failures


def main(argv: list) -> int:
    try:
        import xmlschema
    except ImportError:
        print("xmlschema is required: pip install xmlschema", file=sys.stderr)
        return 2

    here = Path(__file__).resolve().parent
    if argv:
        dirs = [Path(a).resolve() for a in argv]
    else:
        dirs = sorted(d for d in here.iterdir()
                      if d.is_dir() and (d / "manifest.xml").exists())

    if not dirs:
        print("no connector directories found", file=sys.stderr)
        return 2

    failures = sum(validate_connector(d, xmlschema) for d in dirs)

    print()
    if failures:
        print(f"{failures} problem(s) found")
        return 1
    print(f"{len(dirs)} connector(s) validate against Tableau's XSDs")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
