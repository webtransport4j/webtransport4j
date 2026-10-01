#!/usr/bin/env python3
"""Validate invariants of a WebTransport4J release JAR and its published POM."""

from __future__ import annotations

import struct
import sys
import zipfile
from collections import Counter
from pathlib import Path


BASELINE_CLASS_VERSION = 52  # Java 8
JAVA_25_CLASS_VERSION = 69
VERSIONED_PREFIX = "META-INF/versions/25/"


def fail(message: str) -> None:
    print(f"release artifact validation failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def class_major(data: bytes, name: str) -> int:
    if len(data) < 8 or data[:4] != b"\xca\xfe\xba\xbe":
        fail(f"{name} is not a valid class file")
    return struct.unpack(">H", data[6:8])[0]


def validate_jar(path: Path) -> None:
    with zipfile.ZipFile(path) as jar:
        entries = jar.infolist()
        names = [entry.filename for entry in entries]

        duplicates = sorted(
            name for name, count in Counter(names).items() if count > 1
        )
        if duplicates:
            fail(f"duplicate ZIP entries: {', '.join(duplicates)}")

        try:
            manifest = jar.read("META-INF/MANIFEST.MF").decode("utf-8")
        except KeyError:
            fail("META-INF/MANIFEST.MF is missing")

        normalized_manifest = manifest.replace("\r\n", "\n").lower()
        if "multi-release: true\n" not in normalized_manifest:
            fail("manifest does not declare Multi-Release: true")

        class_names = [name for name in names if name.endswith(".class")]
        if not class_names:
            fail("JAR contains no class files")

        versioned_classes = [
            name for name in class_names if name.startswith("META-INF/versions/")
        ]
        if not versioned_classes:
            fail("JAR contains no Java 25 versioned classes")

        unexpected_versions = [
            name
            for name in versioned_classes
            if not name.startswith(VERSIONED_PREFIX)
        ]
        if unexpected_versions:
            fail(
                "classes found under an unexpected MR-JAR version: "
                + ", ".join(unexpected_versions)
            )

        for name in class_names:
            major = class_major(jar.read(name), name)
            if name.startswith(VERSIONED_PREFIX):
                if major != JAVA_25_CLASS_VERSION:
                    fail(f"{name} has class version {major}, expected 69")
            elif major > BASELINE_CLASS_VERSION:
                fail(f"{name} has class version {major}, expected at most 52")


def validate_pom(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    forbidden = ("${os.detected.classifier}", "os-maven-plugin")
    found = [value for value in forbidden if value in text]
    if found:
        fail("POM contains non-portable OS detection: " + ", ".join(found))


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: verify-release-artifact.py <jar> <pom>")

    jar_path = Path(sys.argv[1])
    pom_path = Path(sys.argv[2])
    if not jar_path.is_file():
        fail(f"JAR not found: {jar_path}")
    if not pom_path.is_file():
        fail(f"POM not found: {pom_path}")

    validate_jar(jar_path)
    validate_pom(pom_path)
    print(f"validated release artifact: {jar_path}")


if __name__ == "__main__":
    main()
