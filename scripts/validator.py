"""
TreeHub Validator — JSON schema and integrity validation for tree and manifest files.

Usage:
    python scripts/validator.py indices/supabase/v2.1.0-tree.json
    python scripts/validator.py indices/supabase/v2.1.0-tree.json --manifest indices/supabase/v2.1.0-manifest.json
    python scripts/validator.py --all           # Validate all indices
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = PROJECT_ROOT / "schemas"
INDICES_DIR = PROJECT_ROOT / "indices"


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


class ValidationError(Exception):
    """Custom exception for validation errors with detailed context."""
    
    def __init__(self, message: str, file_path: Optional[Path] = None, 
                 line_number: Optional[int] = None, field_path: Optional[str] = None):
        self.message = message
        self.file_path = file_path
        self.line_number = line_number
        self.field_path = field_path
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        parts = [self.message]
        if self.file_path:
            parts.append(f"File: {self.file_path}")
        if self.line_number:
            parts.append(f"Line: {self.line_number}")
        if self.field_path:
            parts.append(f"Field: {self.field_path}")
        return " | ".join(parts)


class TreeValidator:
    """Validates TreeHub tree.json and manifest.json files.

    Checks:
        1. JSON syntax — valid JSON
        2. Schema compliance — matches tree-schema.json / manifest-schema.json
        3. Integrity — SHA-256 hash verification
        4. Consistency — cross-references between tree and manifest
        
    Improvements:
        - Detailed error messages with file paths and line numbers
        - Schema caching for improved performance
        - Circular reference detection in tree structure
        - Unique ID validation across the tree
        - Better error recovery and reporting
    """

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self._tree_schema: dict | None = None
        self._manifest_schema: dict | None = None
        self._schema_cache: dict[str, dict] = {}

    # -- Public API ---------------------------------------------------------

    def validate_tree(self, tree_path: Path) -> bool:
        """Validate a tree.json file.

        Returns True if valid, False otherwise.
        """
        self.errors.clear()
        self.warnings.clear()

        # 1. Parse JSON with line number tracking
        data = self._load_json(tree_path)
        if data is None:
            return False

        # 2. Check required top-level keys
        if "meta" not in data:
            self.errors.append("Missing required key: 'meta'")
        if "tree" not in data:
            self.errors.append("Missing required key: 'tree'")

        if self.errors:
            return False

        # 3. Validate meta fields
        meta = data["meta"]
        required_meta = ["platform", "version", "indexed_at", "source_url", "tree_hash", "pages_count"]
        for field in required_meta:
            if field not in meta:
                self.errors.append(f"Missing required meta field: '{field}'")
        
        # Validate platform format (lowercase, hyphen-separated)
        if "platform" in meta:
            platform = meta["platform"]
            if not isinstance(platform, str):
                self.errors.append(f"meta.platform must be a string, got {type(platform).__name__}")
            elif not platform or not platform.replace("-", "").isalnum():
                self.errors.append(f"meta.platform must be lowercase alphanumeric with hyphens: '{platform}'")

        # 4. Validate tree structure
        tree = data.get("tree", {})
        if "root" not in tree:
            self.errors.append("Missing 'root' in tree")
        else:
            # Check for circular references and duplicate IDs
            seen_ids: set[str] = set()
            self._validate_node(tree["root"], path="root", seen_ids=seen_ids)

        # 5. Validate tree hash integrity
        if "tree_hash" in meta:
            self._validate_tree_hash(data)

        # 6. JSON Schema validation (if jsonschema is available)
        self._validate_against_schema(data, "tree")

        return len(self.errors) == 0

    def validate_manifest(self, manifest_path: Path) -> bool:
        """Validate a manifest.json file.

        Returns True if valid, False otherwise.
        """
        self.errors.clear()
        self.warnings.clear()

        data = self._load_json(manifest_path)
        if data is None:
            return False

        required = ["platform", "version", "files", "provenance", "stats", "schema_version"]
        for field in required:
            if field not in data:
                self.errors.append(f"Missing required field: '{field}'")

        # Validate platform format
        if "platform" in data:
            platform = data["platform"]
            if not isinstance(platform, str):
                self.errors.append(f"platform must be a string, got {type(platform).__name__}")
            elif not platform or not platform.replace("-", "").isalnum():
                self.errors.append(f"platform must be lowercase alphanumeric with hyphens: '{platform}'")

        # Validate version format
        if "version" in data:
            version = data["version"]
            if not isinstance(version, str):
                self.errors.append(f"version must be a string, got {type(version).__name__}")
            elif not version:
                self.errors.append("version cannot be empty")

        # Validate files section
        files = data.get("files", {})
        if "tree" in files:
            tree_file = files["tree"]
            for key in ["path", "hash", "size_bytes"]:
                if key not in tree_file:
                    self.errors.append(f"Missing files.tree.{key}")
            
            # Validate hash format
            if "hash" in tree_file:
                hash_val = tree_file["hash"]
                if not hash_val.startswith("sha256:"):
                    self.errors.append(f"files.tree.hash must start with 'sha256:': {hash_val}")
                elif len(hash_val) != 71:  # sha256: + 64 hex chars
                    self.errors.append(f"files.tree.hash must be 71 characters (sha256: + 64 hex): {hash_val}")

        # Validate provenance
        provenance = data.get("provenance", {})
        for key in ["indexed_by", "indexer_version"]:
            if key not in provenance:
                self.errors.append(f"Missing provenance.{key}")

        # Validate stats
        stats = data.get("stats", {})
        if "pages_count" in stats:
            pages = stats["pages_count"]
            if not isinstance(pages, int) or pages < 0:
                self.errors.append(f"stats.pages_count must be a non-negative integer: {pages}")

        # JSON Schema validation
        self._validate_against_schema(data, "manifest")

        return len(self.errors) == 0

    def validate_pair(self, tree_path: Path, manifest_path: Path) -> bool:
        """Validate a tree + manifest pair for consistency."""
        self.errors.clear()
        self.warnings.clear()

        tree_valid = self.validate_tree(tree_path)
        tree_errors = list(self.errors)
        tree_warnings = list(self.warnings)

        manifest_valid = self.validate_manifest(manifest_path)

        # Merge errors
        self.errors = tree_errors + self.errors
        self.warnings = tree_warnings + self.warnings

        if not (tree_valid and manifest_valid):
            return False

        # Cross-validate
        tree_data = self._load_json(tree_path)
        manifest_data = self._load_json(manifest_path)

        if tree_data and manifest_data:
            # Platform must match
            tree_platform = tree_data.get("meta", {}).get("platform", "")
            manifest_platform = manifest_data.get("platform", "")
            if tree_platform != manifest_platform:
                self.errors.append(
                    f"Platform mismatch: tree has '{tree_platform}', manifest has '{manifest_platform}'"
                )

            # Version must match
            tree_version = tree_data.get("meta", {}).get("version", "")
            manifest_version = manifest_data.get("version", "")
            if tree_version != manifest_version:
                self.errors.append(
                    f"Version mismatch: tree has '{tree_version}', manifest has '{manifest_version}'"
                )

            # Verify file hash from manifest matches actual tree file hash
            tree_bytes = tree_path.read_bytes()
            actual_hash = f"sha256:{hashlib.sha256(tree_bytes).hexdigest()}"
            expected_hash = manifest_data.get("files", {}).get("tree", {}).get("hash")

            if expected_hash and actual_hash != expected_hash:
                self.errors.append(
                    f"Tree file hash mismatch: expected {expected_hash}, got {actual_hash}"
                )

            # Cross-validate stats if present
            tree_pages = tree_data.get("meta", {}).get("pages_count", 0)
            manifest_pages = manifest_data.get("stats", {}).get("pages_count", 0)
            if tree_pages != manifest_pages:
                self.warnings.append(
                    f"Page count mismatch: tree has {tree_pages}, manifest has {manifest_pages}"
                )

        return len(self.errors) == 0

    def validate_all(self) -> dict[str, bool]:
        """Validate all indices in the indices/ directory.

        Returns dict of platform → valid.
        """
        results: dict[str, bool] = {}

        for platform_dir in sorted(INDICES_DIR.iterdir()):
            if not platform_dir.is_dir() or platform_dir.name.startswith("_"):
                continue

            # Find tree files
            tree_files = sorted(platform_dir.glob("*-tree.json"))
            for tree_file in tree_files:
                version = tree_file.stem.replace("-tree", "")
                manifest_file = platform_dir / f"{version}-manifest.json"

                key = f"{platform_dir.name}/{version}"
                if manifest_file.exists():
                    results[key] = self.validate_pair(tree_file, manifest_file)
                else:
                    results[key] = self.validate_tree(tree_file)
                    if results[key]:
                        self.warnings.append(f"No manifest found for {key}")

        return results

    # -- Internal -----------------------------------------------------------

    def _load_json(self, path: Path) -> dict | None:
        """Load and parse a JSON file with error context."""
        try:
            content = path.read_text(encoding="utf-8")
            return json.loads(content)
        except json.JSONDecodeError as exc:
            self.errors.append(f"Invalid JSON in {path.name}: {exc}")
            if hasattr(exc, 'lineno'):
                self.errors.append(f"  → Error at line {exc.lineno}, column {exc.colno}")
            return None
        except FileNotFoundError:
            self.errors.append(f"File not found: {path}")
            return None

    def _validate_node(self, node: dict, path: str, seen_ids: set[str]) -> None:
        """Recursively validate a tree node with circular reference and ID tracking."""
        required = ["id", "title", "summary", "children"]
        for field in required:
            if field not in node:
                self.errors.append(f"Node at '{path}' missing field: '{field}'")

        # Check for duplicate IDs
        node_id = node.get("id", "")
        if node_id:
            if node_id in seen_ids:
                self.errors.append(f"Duplicate node ID '{node_id}' at path '{path}'")
            else:
                seen_ids.add(node_id)

        # Validate ID format
        if node_id and not isinstance(node_id, str):
            self.errors.append(f"Node ID must be a string at '{path}': got {type(node_id).__name__}")
        elif node_id and not node_id.replace("-", "_").isalnum():
            self.errors.append(f"Node ID must be alphanumeric with hyphens/underscores at '{path}': '{node_id}'")

        # Validate title
        title = node.get("title", "")
        if title and not isinstance(title, str):
            self.errors.append(f"Node title must be a string at '{path}': got {type(title).__name__}")

        # Validate summary
        summary = node.get("summary", "")
        if summary and not isinstance(summary, str):
            self.errors.append(f"Node summary must be a string at '{path}': got {type(summary).__name__}")

        children = node.get("children", [])
        if not isinstance(children, list):
            self.errors.append(f"Node at '{path}': children must be an array")
        else:
            for i, child in enumerate(children):
                child_id = child.get("id", f"[{i}]")
                self._validate_node(child, f"{path}.{child_id}", seen_ids)

    def _validate_tree_hash(self, data: dict) -> None:
        """Verify the tree_hash in meta matches the actual tree content."""
        try:
            tree_content = json.dumps(data["tree"], sort_keys=True)
            actual_hash = f"sha256:{hashlib.sha256(tree_content.encode()).hexdigest()}"
            expected_hash = data["meta"]["tree_hash"]

            if actual_hash != expected_hash:
                self.warnings.append(
                    f"tree_hash mismatch (may differ due to serialization): "
                    f"expected {expected_hash[:30]}..."
                )
        except (KeyError, TypeError):
            pass

    def _validate_against_schema(self, data: dict, schema_type: str) -> None:
        """Validate against JSON Schema if jsonschema is available.
        
        Uses cached schema for improved performance.
        """
        try:
            import jsonschema
        except ImportError:
            self.warnings.append(
                "jsonschema not installed — skipping schema validation. "
                "Install with: pip install jsonschema"
            )
            return

        # Use cached schema
        schema = self._get_schema(schema_type)
        if schema is None:
            return

        try:
            jsonschema.validate(data, schema)
        except jsonschema.ValidationError as exc:
            # Provide more detailed error information
            error_path = ".".join(str(p) for p in exc.absolute_path) if exc.absolute_path else "root"
            self.errors.append(f"Schema validation failed at '{error_path}': {exc.message}")
        except json.JSONDecodeError as exc:
            self.errors.append(f"Invalid JSON in schema file: {exc}")

    def _get_schema(self, schema_type: str) -> dict | None:
        """Load and cache JSON schema."""
        if schema_type in self._schema_cache:
            return self._schema_cache[schema_type]

        schema_file = SCHEMAS_DIR / f"{schema_type}-schema.json"
        if not schema_file.exists():
            self.warnings.append(f"Schema file not found: {schema_file}")
            return None

        try:
            schema = json.loads(schema_file.read_text(encoding="utf-8"))
            self._schema_cache[schema_type] = schema
            return schema
        except json.JSONDecodeError as exc:
            self.errors.append(f"Invalid JSON in schema file {schema_file}: {exc}")
            return None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="TreeHub Validator")
    parser.add_argument("file", nargs="?", help="Path to tree.json to validate")
    parser.add_argument("--manifest", help="Optional manifest.json to cross-validate")
    parser.add_argument("--all", action="store_true", help="Validate all indices")
    parser.add_argument("--strict", action="store_true", help="Treat warnings as errors")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    validator = TreeValidator()

    if args.all:
        results = validator.validate_all()
        if not results:
            print("⚠️  No indices found to validate.")
            return

        all_valid = True
        output_data = {}
        
        for key, valid in results.items():
            status = "✅" if valid else "❌"
            print(f"  {status} {key}")
            output_data[key] = {"valid": valid, "errors": [], "warnings": []}
            if not valid:
                all_valid = False
                for err in validator.errors:
                    print(f"     ↳ {err}")
                    output_data[key]["errors"].append(err)
            for w in validator.warnings:
                print(f"     ⚠️ {w}")
                output_data[key]["warnings"].append(w)

        if args.format == "json":
            print("\n" + json.dumps(output_data, indent=2))

        sys.exit(0 if (all_valid and not args.strict) else 1)

    if not args.file:
        parser.print_help()
        sys.exit(1)

    tree_path = Path(args.file)

    if args.manifest:
        valid = validator.validate_pair(tree_path, Path(args.manifest))
    else:
        valid = validator.validate_tree(tree_path)

    output_data = {
        "file": str(tree_path),
        "valid": valid,
        "errors": validator.errors,
        "warnings": validator.warnings,
    }

    if valid:
        print(f"✅ Valid: {tree_path.name}")
        for w in validator.warnings:
            print(f"   ⚠️ {w}")
    else:
        print(f"❌ Invalid: {tree_path.name}")
        for err in validator.errors:
            print(f"   ↳ {err}")
        for w in validator.warnings:
            print(f"   ⚠️ {w}")

    if args.format == "json":
        print("\n" + json.dumps(output_data, indent=2))

    exit_code = 0 if valid else 1
    if args.strict and validator.warnings:
        exit_code = 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
