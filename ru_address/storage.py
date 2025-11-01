import fnmatch
import glob
import os
import zipfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Iterator, BinaryIO, Optional


class BaseStorage(ABC):
    """Абстрактное хранилище XSD/XML файлов."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def list_regions(self) -> list[str]:
        pass

    @abstractmethod
    @contextmanager
    def open_schema(self, entity: str) -> Iterator[BinaryIO]:
        pass

    @abstractmethod
    @contextmanager
    def open_table(self, table: str, region: Optional[str]) -> Iterator[BinaryIO]:
        pass

    @abstractmethod
    def close(self):
        pass


class DirectoryStorage(BaseStorage):
    """Файлы лежат в распакованном каталоге."""

    def __init__(self, base_path: str):
        self.base_path = base_path

    def _search(self, directory: str, table: str, extension: str) -> str:
        patterns = [
            f'AS_{table}_2*.{extension.lower()}',
            f'AS_{table}_2*.{extension.upper()}',
        ]
        for pattern in patterns:
            matches = glob.glob(os.path.join(directory, pattern))
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                raise FileNotFoundError(f'More than one file found: {os.path.join(directory, pattern)}')
        raise FileNotFoundError(f'Not found source file: {os.path.join(directory, patterns[0])}')

    def list_regions(self) -> list[str]:
        matched = glob.glob('*', root_dir=self.base_path)
        return sorted([f for f in matched if f.isnumeric() and os.path.isdir(os.path.join(self.base_path, f))])

    @contextmanager
    def open_schema(self, entity: str) -> Iterator[BinaryIO]:
        filepath = self._search(self.base_path, entity, 'xsd')
        with open(filepath, 'rb') as fp:
            yield fp

    @contextmanager
    def open_table(self, table: str, region: Optional[str]) -> Iterator[BinaryIO]:
        base = self.base_path
        if region is not None:
            base = os.path.join(self.base_path, region)
        filepath = self._search(base, table, 'xml')
        with open(filepath, 'rb') as fp:
            yield fp

    def close(self):
        # Nothing to close for directory-based storage
        return None


class ZipStorage(BaseStorage):
    """Работа напрямую с ZIP архивом без распаковки."""

    def __init__(self, archive_path: str):
        self.archive_path = archive_path
        self._zip = zipfile.ZipFile(archive_path, 'r')

    def close(self):
        self._zip.close()

    def list_regions(self) -> list[str]:
        regions: set[str] = set()
        for name in self._zip.namelist():
            parts = name.replace('\\', '/').split('/')
            for part in parts[:-1]:
                if part.isdigit():
                    regions.add(part)
        return sorted(regions)

    def _matches(self, name: str, directory: Optional[str], table: str, extension: str) -> bool:
        normalized = name.replace('\\', '/')
        basename = os.path.basename(normalized)
        pattern = f'as_{table.lower()}_2*.{extension.lower()}'
        if not fnmatch.fnmatch(basename.lower(), pattern):
            return False
        path_parts = normalized.split('/')[:-1]
        if directory is None:
            if any(part.isdigit() for part in path_parts):
                return False
        else:
            if directory not in path_parts:
                return False
        return True

    def _find_member(self, directory: Optional[str], table: str, extension: str) -> str:
        candidates = [
            name for name in self._zip.namelist()
            if self._matches(name, directory, table, extension)
        ]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            raise FileNotFoundError(f'More than one archive entry for table {table} ({extension}) in {directory}')
        scope = directory if directory is not None else 'root'
        raise FileNotFoundError(f'Not found source file {table}.{extension} in {scope}')

    @contextmanager
    def open_schema(self, entity: str) -> Iterator[BinaryIO]:
        member = self._find_member(None, entity, 'xsd')
        with self._zip.open(member, 'r') as fp:
            yield fp

    @contextmanager
    def open_table(self, table: str, region: Optional[str]) -> Iterator[BinaryIO]:
        member = self._find_member(region, table, 'xml')
        with self._zip.open(member, 'r') as fp:
            yield fp


def resolve_storage(path: str) -> BaseStorage:
    """Автоматически определяет тип хранилища."""
    if os.path.isdir(path):
        return DirectoryStorage(path)
    if zipfile.is_zipfile(path):
        return ZipStorage(path)
    raise FileNotFoundError(f'Unsupported storage path: {path}')
