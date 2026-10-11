import copy
import csv
import hashlib
import io
import json
import unittest
from scripts import cloud_catalog_recovery as r


PNG_ID = "p" * 32
SIDECAR_ID = "m" * 32
ROOT_ID = "r" * 32
FOLDER_ID = "f" * 32
DIGEST = hashlib.sha256(b"synthetic-fixture").hexdigest()


def fixture():
    data = {"id": "TEST1", "category": "Illustrations", "canonical_sha256": DIGEST,
            "etsy": {"ID": "TEST1", "Filename": "TEST1_example.png", "Title": "Example Bow Charm",
                     "Description": "An original illustrated example design for digital craft projects.",
                     "Tags": "example|bow charm|digital download"}}
    png = {"id": PNG_ID, "name": "TEST1_example.png", "mimeType": "image/png", "size": "100",
           "sha256Checksum": DIGEST, "parents": [FOLDER_ID]}
    sidecar = {"id": SIDECAR_ID, "name": "TEST1_metadata.json", "mimeType": "application/json",
               "parents": [FOLDER_ID]}
    return data, png, sidecar


def record():
    data, png, sidecar = fixture()
    return r.json_product_record(json.dumps(data).encode(), sidecar, [png], ROOT_ID, "example-store")


class RecoveryTests(unittest.TestCase):
    def test_preserves_original_title_description_and_tags(self):
        data, _, _ = fixture()
        result = record()
        self.assertEqual(result["metadata"]["title"], data["etsy"]["Title"])
        self.assertEqual(result["metadata"]["description"], data["etsy"]["Description"])
        self.assertEqual(result["metadata"]["keywords"], ["example", "bow charm", "digital download"])

    def test_exact_file_hash_required(self):
        data, png, sidecar = fixture()
        for digest in ("", "0" * 64):
            png["sha256Checksum"] = digest
            with self.assertRaisesRegex(r.m.ManifestBuilderError, "json_png_hash_mismatch"):
                r.json_product_record(json.dumps(data).encode(), sidecar, [png], ROOT_ID, "example-store")

    def test_duplicate_filename_is_held(self):
        data, png, sidecar = fixture()
        with self.assertRaisesRegex(r.m.ManifestBuilderError, "json_png_name_not_unique"):
            r.json_product_record(json.dumps(data).encode(), sidecar, [png, png], ROOT_ID, "example-store")

    def test_other_parent_not_accepted(self):
        data, png, sidecar = fixture()
        png["parents"] = [ROOT_ID]
        with self.assertRaisesRegex(r.m.ManifestBuilderError, "json_parent_mismatch"):
            r.json_product_record(json.dumps(data).encode(), sidecar, [png], ROOT_ID, "example-store")

    def test_paths_and_wrong_metadata_identity_rejected(self):
        for field, value in (("Filename", "../TEST1_example.png"), ("ID", "WRONG")):
            data, png, sidecar = fixture()
            data["etsy"][field] = value
            with self.assertRaises(r.m.ManifestBuilderError):
                r.json_product_record(json.dumps(data).encode(), sidecar, [png], ROOT_ID, "example-store")

    def test_explicit_rights_hold_not_overridden(self):
        data, png, sidecar = fixture()
        data["rights_gate"] = "review"
        with self.assertRaisesRegex(r.m.ManifestBuilderError, "json_explicit_rights_hold"):
            r.json_product_record(json.dumps(data).encode(), sidecar, [png], ROOT_ID, "example-store")

    def test_marketplace_pending_not_mistaken_for_site_ban(self):
        data, png, sidecar = fixture()
        data["etsy"]["State"] = "prepared_metadata_only_not_uploaded"
        self.assertIsNotNone(r.json_product_record(json.dumps(data).encode(), sidecar, [png], ROOT_ID, "example-store"))

    def test_append_preserves_previous_objects(self):
        old = record()
        new = copy.deepcopy(old)
        new["product_identity"] = "gdrive:v3:" + "a" * 64
        new["assets"]["png_drive_file_id"] = "x" * 32
        before = copy.deepcopy(old)
        output = r.append_only_union([old], [new])
        self.assertEqual(output, [before, new])
        self.assertIs(output[0], old)

    def test_existing_source_cannot_be_added_under_new_identity(self):
        old = record()
        new = copy.deepcopy(old)
        new["product_identity"] = "gdrive:v3:" + "a" * 64
        with self.assertRaisesRegex(r.m.ManifestBuilderError, "append_identity_collision"):
            r.append_only_union([old], [new])

    def test_nested_folders_are_scanned(self):
        _, png, _ = fixture()
        class Drive:
            def list_children_many(self, ids):
                content = {ROOT_ID: [{"id": FOLDER_ID, "name": "collection", "mimeType": r.m.FOLDER_MIME}], FOLDER_ID: [png]}
                return {key: content[key] for key in ids}
        folders, children, stats = r.scan_tree(lambda: Drive(), ROOT_ID)
        self.assertEqual(folders[FOLDER_ID]["depth"], 1)
        self.assertEqual(children[FOLDER_ID], [png])
        self.assertEqual(stats["png_files"], 1)

    def test_no_local_runtime_entry(self):
        with self.assertRaisesRegex(r.p.PipelineError, "cloud_runtime_required"):
            r.p.enforce_cloud_runtime({})

    def test_canonical_csv_discovery_and_repeat_skip(self):
        _, png, _ = fixture()
        values = {"DeliveryFile": png["name"], "DeliveryKind": "transparent_png",
            "OwnerApproval": "approved", "EtsyStage": "active_verified",
            "EtsyTitle": "Example Bow Charm", "EtsyDescription": "An original illustration for digital craft projects.",
            "EtsyTags": "example, illustration", "PNG_Bytes": "100", "PNG_SHA256": DIGEST}
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=list(values))
        writer.writeheader()
        writer.writerow(values)
        payload = output.getvalue().encode()
        sidecar = {"id": SIDECAR_ID, "name": r.p.METADATA_FILENAME, "mimeType": "text/csv",
                   "parents": [FOLDER_ID], "size": str(len(payload))}
        class Drive:
            def download_bytes(self, file_id, maximum_bytes):
                return payload, sidecar
        children = {FOLDER_ID: [png, sidecar]}
        added, holds = r.discover(Drive(), ROOT_ID, "example-store", children, [])
        self.assertEqual(holds, [])
        self.assertEqual(len(added), 1)
        again, holds = r.discover(Drive(), ROOT_ID, "example-store", children, added)
        self.assertEqual((again, holds), ([], []))


if __name__ == "__main__":
    unittest.main()
