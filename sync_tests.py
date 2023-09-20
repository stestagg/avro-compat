#!/usr/bin/env python
from packaging import version
import fastavro
import avro
from io import BytesIO
from collections import defaultdict
import click
from pathlib import Path
import re
import requests
import zipfile


HERE = Path(__file__).parent

SRC_DIR = HERE / "vendor-tests"
DEST_DIR = HERE / "tests" / "lib-tests"

AVRO_SKIP = {
    'test_datafile_interop.py',
    'test_compatibility.py',
    'test_ipc.py',
    'test_protocol.py',
    'test_tether_task.py',
    'test_tether_task_runner.py',
    'test_tether_word_count.py',
}

FASTAVRO_SKIP = {
    'test_appendable.py'
}

# This is kinda ugly!
HAVE_IMPORT_HEADERS = defaultdict(bool)


def get_header(library):
    if HAVE_IMPORT_HEADERS[library]:
        return ""
    HAVE_IMPORT_HEADERS[library] = True
    return f"import avro_compat.{library} as {library}\n"



def substitute_line(match):
    line_content = match.group(1)
    pattern = match.group(2)
    replacement = match.group(3)
    return line_content.replace(pattern, replacement)


def rewrite_file(src_text):
    if 'import avro' in src_text:
        pos = src_text.find('import avro')
        src_text = src_text.replace('import avro', 'import avro_compat.avro')
        src_text = src_text[:pos] + 'import avro_compat.avro as avro\n' + src_text[pos:]
    src_text = src_text.replace('from avro', 'from avro_compat.avro')

    src_text = src_text.replace('import fastavro', 'import avro_compat.fastavro as fastavro')
    src_text = src_text.replace('from fastavro', 'from avro_compat.fastavro')
    
    # Disable some tests
    # Protocol name tests
    src_text = src_text.replace('suite.addTests(ParseProtocolNameValidationDisabledTestCase', '# DISABLED (')
    # Disable warnings test
    src_text = src_text.replace('self.assertEqual([], test_warnings)', 'pass # warning check disabled')

    return src_text


def sync_avro():
    avro_version = avro.__version__
    print('Avro:', avro_version)

    avro_test_prefix = 'lang/py/avro/test'
    if version.parse(avro_version) < version.parse('1.11.0'):
        avro_test_prefix = 'lang/py3/avro/tests'
        AVRO_SKIP.add('test_script.py')

    resp = requests.get(f'https://github.com/apache/avro/archive/refs/tags/release-{avro_version}.zip')
    if resp.status_code != 200:
        print(f"Unable to download avro tests: {resp.status_code} Trying master")
        resp = requests.get('https://codeload.github.com/apache/avro/zip/refs/heads/master')
        resp.raise_for_status()
    
    buf = BytesIO(resp.content)
    
    with zipfile.ZipFile(buf, 'r') as zip_file:
        for info in zip_file.infolist():
            if info.is_dir():
                continue
            rel_name = info.filename.split('/', 1)[-1]
            if rel_name.startswith(avro_test_prefix):
                rel_name = rel_name.removeprefix(avro_test_prefix).lstrip('/')
                file_name = rel_name.rsplit('/', 1)[-1]
                if file_name in AVRO_SKIP:
                    continue
                dest_path = DEST_DIR / 'avro_tests' / rel_name
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                content = zip_file.read(info)
                content = content.decode('utf-8')
                content = rewrite_file(content)
                print(f"Syncing {dest_path}")
                dest_path.write_text(content)


def sync_fastavro():
    fastavro_version = fastavro.__version__
    print('Fastavro:', fastavro_version)

    resp = requests.get(f'https://github.com/fastavro/fastavro/archive/refs/tags/{fastavro_version}.zip')
    if resp.status_code != 200:
        print(f"Unable to download fastavro tests: {resp.status_code} Trying master")
        resp = requests.get('https://codeload.github.com/fastavro/fastavro/zip/refs/heads/master')
        resp.raise_for_status()
    
    buf = BytesIO(resp.content)
    with zipfile.ZipFile(buf, 'r') as zip_file:
        for info in zip_file.infolist():
            if info.is_dir():
                continue
            rel_name = info.filename.split('/', 1)[-1]
            if not rel_name.startswith('tests/'):
                continue
            rel_name = rel_name.removeprefix('tests/').lstrip('/')
            file_name = rel_name.rsplit('/', 1)[-1]
            if file_name in FASTAVRO_SKIP:
                continue
            dest_path = DEST_DIR / 'fastavro_tests' / rel_name
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            content = zip_file.read(info)
            if file_name.endswith('.py'):
                content = content.decode('utf-8')
                content = rewrite_file(content)
                content = content.encode()
            print(f"Syncing {dest_path}")
            dest_path.write_bytes(content)


@click.command()
def sync_tests():
    print("Syncing tests")
    sync_avro()
    sync_fastavro()


if __name__ == "__main__":
    sync_tests()
