#!/usr/bin/env python
from collections import defaultdict
import click
from pathlib import Path
import re

HERE = Path(__file__).parent

SRC_DIR = HERE / "lib-tests"
DEST_DIR = HERE / "tests" / "lib-tests"

# This is kinda ugly!
HAVE_IMPORT_HEADERS = defaultdict(bool)


def get_header(library):
    if HAVE_IMPORT_HEADERS[library]:
        return ""
    HAVE_IMPORT_HEADERS[library] = True
    return f"import avro_compat.{library} as {library}\n"


def switch_import(match):
    stmt = match.group(1)
    if stmt.startswith("import "):
        library = stmt.removeprefix("import ").split(".")[0]
        out = get_header(library) + stmt.replace("import ", "import avro_compat.")
        return out
    elif stmt.startswith("from "):
        return stmt.replace("from ", "from avro_compat.")
    raise NotImplementedError(stmt)


@click.command()
def sync_tests():
    print("Syncing tests")
    test_files = list(SRC_DIR.glob("**/test_*.py"))
    for test_file in test_files:
        print(f"Syncing {test_file}")
        HAVE_IMPORT_HEADERS.clear()
        test_rel = test_file.relative_to(SRC_DIR)

        dest_file = DEST_DIR / test_rel
        dest_file.parent.mkdir(parents=True, exist_ok=True)

        src_text = test_file.read_text()
        modified_text = re.sub(r"^(.*)\s+#\s*switch-compat\s*$", switch_import, src_text, flags=re.M)
        dest_file.write_text(modified_text)


if __name__ == "__main__":
    sync_tests()
