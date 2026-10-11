import io
import json
import unittest
import zipfile
from unittest.mock import Mock, patch

from scripts import cloud_catalog_archive_recovery as a
from scripts import cloud_catalog_recovery as r


def row():
    return {"identity": "z" * 32 + "|artboard 1", "source_kind": "archive_svg_member",
        "archive_file_id": "z" * 32, "archive_member": "artboard 1", "source_file_id": "",
        "canonical_folder": "Library/collection.zip/SVG/Artboard 1.svg", "source_name": "Artboard 1.svg",
        "screening_evidence": a.ARCHIVE_SCREENING, "ai_provenance": "Human-original/non-generative SVG campaign",
        "metadata_recovery_status": "complete_recovered", "rights_gate": "Campaign-gated; preserve evidence",
        "title": "Original floral illustration", "description": "An original floral design for creative projects.",
        "keywords": "floral, illustration, craft", "category": "Arts"}


def payload(rows):
    return "\n".join(json.dumps(item) for item in rows).encode()


class ArchiveRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.meta = {"id": "z" * 32, "name": "collection.zip", "mimeType": "application/zip",
                     "parents": ["f" * 32], "size": "1000", "md5Checksum": "a" * 32}
        self.drive = Mock()
        self.drive.get_metadata.return_value = self.meta
        self.entry = zipfile.ZipInfo("SVG/Artboard 1.svg")
        self.entry.file_size, self.entry.compress_size = 100, 50

    def discover(self, rows=None, claimed=None, entries=None):
        return a.discover(lambda: self.drive, payload(rows if rows is not None else [row()]), "example-store",
            claimed or set(), lambda *args: entries if entries is not None else [self.entry])

    def test_full_path_and_metadata_preserved_and_repeat_skips_index(self):
        records, report = self.discover()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["metadata"]["title"], row()["title"])
        self.assertEqual(record["assets"]["archive_svg_member"], "SVG/Artboard 1.svg")
        self.assertIsNone(record["assets"]["archive_png_member"])
        self.assertTrue(record["assets"]["render_needed"])
        self.assertEqual(report["holds"], [])
        reader = Mock(side_effect=AssertionError("already claimed ZIP must not be read"))
        result, repeated = a.discover(lambda: self.drive, payload([row()]), "example-store", r.source_ids(record), reader)
        self.assertEqual(result, [])
        self.assertEqual(repeated["existing_rows"], 1)

    def test_basename_is_not_sufficient(self):
        other = zipfile.ZipInfo("Elsewhere/Artboard 1.svg")
        other.file_size, other.compress_size = 100, 50
        records, report = self.discover(entries=[other])
        self.assertEqual(records, [])
        self.assertIn("archive_exact_member_not_unique", report["hold_reasons"])

    def test_missing_metadata_or_screen_or_rights_hold_never_pass(self):
        for key, value in (("description", ""), ("keywords", ""), ("metadata_recovery_status", "partial_recovered"),
            ("screening_evidence", "local=clear"), ("screening_evidence", a.ARCHIVE_SCREENING.replace("terminal", "pending")),
            ("rights_gate", "blocked"), ("historical_hold_reason", "copyright review"), ("source_file_id", "s" * 32)):
            with self.subTest(key=key, value=value):
                records, report = self.discover([{**row(), key: value}])
                self.assertEqual(records, [])
                self.assertEqual(len(report["holds"]), 1)

    def test_no_traversal_backslash_repeated_archive_or_wrong_identity(self):
        for path in ("Library/collection.zip/../Artboard 1.svg", "Library/collection.zip/SVG\\Artboard 1.svg",
                     "Library/collection.zip/collection.zip/SVG/Artboard 1.svg", "Library/other.zip/SVG/Artboard 1.svg"):
            records, report = self.discover([{**row(), "canonical_folder": path}])
            self.assertEqual(records, [])
            self.assertEqual(len(report["holds"]), 1)

    def test_duplicate_or_encrypted_or_symlink_members_never_publish(self):
        self.assertEqual(self.discover(entries=[self.entry, self.entry])[0], [])
        self.entry.flag_bits = 1
        self.assertEqual(self.discover()[0], [])
        self.entry.flag_bits = 0
        self.entry.external_attr = 0o120777 << 16
        self.assertEqual(self.discover()[0], [])

    def test_append_union_rejects_archive_alias_collision(self):
        records, _ = self.discover()
        alias = {**records[0], "product_identity": "different-identity"}
        with self.assertRaisesRegex(a.m.ManifestBuilderError, "append_identity_collision"):
            r.append_only_union(records, [alias])

    def test_range_reader_is_bounded_and_checks_length(self):
        data = b"abcdefghij"
        reader = a.IndexReader(len(data), lambda start, end: data[start:end + 1])
        reader.seek(-3, 2)
        self.assertEqual(reader.read(), b"hij")
        self.assertEqual(reader.read(), b"")
        with self.assertRaises(ValueError):
            reader.seek(-1)
        with self.assertRaisesRegex(a.m.ManifestBuilderError, "read_limit"):
            a.IndexReader(a.MAX_INDEX_BYTES + 1, Mock()).read()
        with self.assertRaisesRegex(a.m.ManifestBuilderError, "length_invalid"):
            a.IndexReader(3, lambda *args: b"").read()

    def test_zip_directory_can_be_read_without_extracting_artwork(self):
        # Pure in-memory fixture, no artwork or filesystem writes.
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as z:
            z.writestr("entry.txt", b"test")
        data = buffer.getvalue()
        with zipfile.ZipFile(a.IndexReader(len(data), lambda start, end: data[start:end + 1])) as z:
            self.assertEqual(z.namelist(), ["entry.txt"])

    def test_cloud_ranges_reject_full_response_mismatch_overflow_and_changed_source(self):
        for status, content_range, body, changed, error in (
            (200, "bytes 0-2/1000", b"abc", False, "range_response_invalid"),
            (206, "bytes 0-3/1000", b"abc", False, "range_identity_invalid"),
            (206, "bytes 0-2/1000", b"abcd", False, "range_overflow"),
            (206, "bytes 0-2/1000", b"abc", True, "changed_during_index"),
        ):
            response = Mock(status_code=status, headers={"Content-Range": content_range})
            response.iter_content.return_value = [body]
            session = Mock()
            session.get.return_value.__enter__ = Mock(return_value=response)
            session.get.return_value.__exit__ = Mock(return_value=False)
            self.drive.get_metadata.return_value = {**self.meta, "md5Checksum": "b" * 32} if changed else self.meta
            with patch.object(a.p, "enforce_cloud_runtime"), patch("google.auth.transport.requests.AuthorizedSession") as factory:
                factory.return_value.__enter__.return_value = session
                with self.assertRaisesRegex(a.m.ManifestBuilderError, error):
                    with a.cloud_reader(self.drive, self.meta) as reader:
                        reader.read(3)

    def test_extract_selected_reuses_guards_with_memory_destinations(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as z:
            z.writestr("SVG/Artboard 1.svg", b"fixture-text-only")
            z.writestr("unrelated.txt", b"not selected")
        data = buffer.getvalue()
        destination = Mock()
        sink = io.BytesIO()
        destination.open.return_value.__enter__ = Mock(return_value=sink)
        destination.open.return_value.__exit__ = Mock(return_value=False)
        with patch.object(a, "cloud_reader") as reader:
            reader.return_value.__enter__.return_value = a.IndexReader(len(data), lambda start, end: data[start:end + 1])
            result = a.extract_selected(self.drive, self.meta, svg_member="SVG/Artboard 1.svg", png_member=None,
                svg_destination=destination, png_destination=Mock())
        self.assertEqual(result, (False, True))
        self.assertEqual(sink.getvalue(), b"fixture-text-only")


if __name__ == "__main__":
    unittest.main()
