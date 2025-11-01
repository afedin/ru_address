import os
import tempfile
import unittest
import zipfile
from types import SimpleNamespace
from unittest import mock
import sys

try:
    import lxml.etree  # noqa: F401
    LXML_AVAILABLE = True
except ModuleNotFoundError:
    LXML_AVAILABLE = False
    etree_stub = SimpleNamespace()

    def _missing(*args, **kwargs):
        raise RuntimeError('lxml is required for this operation')

    etree_stub.iterparse = _missing
    etree_stub.parse = _missing
    sys.modules.setdefault('lxml', SimpleNamespace(etree=etree_stub))
    sys.modules['lxml.etree'] = etree_stub


class _DummyProcess:
    def __init__(self, _pid):
        pass

    def memory_info(self):
        return SimpleNamespace(rss=0)


sys.modules.setdefault('psutil', SimpleNamespace(Process=_DummyProcess))

HAS_PIPELINE = sys.version_info >= (3, 10)

if HAS_PIPELINE:
    from ru_address.pipeline import _process_region, DatabaseConfig  # noqa: E402
else:
    DatabaseConfig = None  # type: ignore

from ru_address.storage import ZipStorage  # noqa: E402


XSD_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="AddrObj">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="Object" minOccurs="0" maxOccurs="unbounded">
          <xs:complexType>
            <xs:attribute name="ID" type="xs:string" />
            <xs:attribute name="NAME" type="xs:string" />
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""

XML_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<AddrObj>
  <Object ID="1" NAME="Test" />
</AddrObj>
"""


def build_archive(base_dir: str) -> str:
    archive_path = os.path.join(base_dir, 'sample.zip')
    with zipfile.ZipFile(archive_path, 'w') as archive:
        archive.writestr('schema/AS_ADDR_OBJ.XSD', XSD_CONTENT)
        archive.writestr('77/AS_ADDR_OBJ_20250131.XML', XML_CONTENT)
    return archive_path


class ZipStorageTest(unittest.TestCase):
    def test_zip_storage_lists_regions_and_opens_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = build_archive(tmpdir)
            storage = ZipStorage(archive_path)
            try:
                self.assertEqual(storage.list_regions(), ['77'])
                with storage.open_schema('ADDR_OBJ') as schema_stream:
                    data = schema_stream.read()
                    self.assertIn(b'AddrObj', data)
                with storage.open_table('ADDR_OBJ', '77') as table_stream:
                    data = table_stream.read()
                    self.assertIn(b'Object', data)
            finally:
                storage.close()


class DirectoryStorageTest(unittest.TestCase):
    def test_directory_storage_handles_current_naming(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_path = os.path.join(tmpdir, 'AS_ADDR_OBJ.XSD')
            with open(schema_path, 'w', encoding='utf-8') as fp:
                fp.write(XSD_CONTENT)
            region_dir = os.path.join(tmpdir, '77')
            os.makedirs(region_dir, exist_ok=True)
            with open(os.path.join(region_dir, 'AS_ADDR_OBJ_20250131.XML'), 'w', encoding='utf-8') as fp:
                fp.write(XML_CONTENT)

            from ru_address.storage import DirectoryStorage  # local import to avoid cycles

            storage = DirectoryStorage(tmpdir)
            try:
                self.assertEqual(storage.list_regions(), ['77'])
                with storage.open_schema('ADDR_OBJ') as schema_stream:
                    data = schema_stream.read()
                    self.assertIn(b'AddrObj', data)
                with storage.open_table('ADDR_OBJ', '77') as table_stream:
                    data = table_stream.read()
                    self.assertIn(b'Object', data)
            finally:
                storage.close()


@unittest.skipUnless(LXML_AVAILABLE and HAS_PIPELINE, 'pipeline region test requires Python 3.10+ and lxml')
class PipelineRegionTest(unittest.TestCase):
    def test_process_region_creates_dump_and_invokes_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = build_archive(tmpdir)
            db_config = DatabaseConfig(database='gar')

            called = {}

            def fake_import(config, dump_path):
                called['config'] = config
                called['dump_path'] = dump_path
                self.assertTrue(os.path.exists(dump_path))

            with mock.patch('ru_address.pipeline._import_dump', side_effect=fake_import):
                _process_region(archive_path, '77', ['ADDR_OBJ'], db_config)

            self.assertIn('config', called)
            self.assertEqual(called['config'].database, 'gar')
