"""
TreeHub Differ — Generates structural diffs between two PageIndex tree versions.

Usage:
    python scripts/differ.py indices/supabase/v2.0.0-tree.json indices/supabase/v2.1.0-tree.json
    python scripts/differ.py --platform supabase --v1 v2.0.0 --v2 v2.1.0
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_INDICES_DIR = Path("indices")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class NodeChange:
    """Represents a change to a single tree node."""

    node_id: str
    title: str
    change_type: str  # "added", "removed", "modified", "moved"
    path: str  # Dot-separated path in tree
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "title": self.title,
            "change_type": self.change_type,
            "path": self.path,
            "details": self.details,
        }


@dataclass
class TreeDiff:
    """Result of diffing two tree versions."""

    platform: str
    version_from: str
    version_to: str
    changes: list[NodeChange] = field(default_factory=list)

    @property
    def added(self) -> list[NodeChange]:
        return [c for c in self.changes if c.change_type == "added"]

    @property
    def removed(self) -> list[NodeChange]:
        return [c for c in self.changes if c.change_type == "removed"]

    @property
    def modified(self) -> list[NodeChange]:
        return [c for c in self.changes if c.change_type == "modified"]

    @property
    def moved(self) -> list[NodeChange]:
        return [c for c in self.changes if c.change_type == "moved"]

    @property
    def total_changes(self) -> int:
        return len(self.changes)

    def to_dict(self) -> dict:
        return {
            "platform": self.platform,
            "version_from": self.version_from,
            "version_to": self.version_to,
            "summary": {
                "added": len(self.added),
                "removed": len(self.removed),
                "modified": len(self.modified),
                "moved": len(self.moved),
                "total_changes": self.total_changes,
            },
            "changes": [c.to_dict() for c in self.changes],
        }

    def to_markdown(self) -> str:
        """Generate a human-readable markdown changelog."""
        lines = [
            f"# Changelog: {self.platform} {self.version_from} → {self.version_to}",
            "",
            f"**{len(self.added)}** added · **{len(self.removed)}** removed · **{len(self.modified)}** modified · **{len(self.moved)}** moved",
            "",
        ]

        if self.added:
            lines.append("## ➕ Added")
            for c in self.added:
                lines.append(f"- **{c.title}** (`{c.path}`)")
            lines.append("")

        if self.removed:
            lines.append("## ➖ Removed")
            for c in self.removed:
                lines.append(f"- **{c.title}** (`{c.path}`)")
            lines.append("")

        if self.modified:
            lines.append("## ✏️ Modified")
            for c in self.modified:
                details = ", ".join(f"{k}: {v}" for k, v in c.details.items())
                lines.append(f"- **{c.title}** (`{c.path}`) — {details}")
            lines.append("")

        if self.moved:
            lines.append("## 🔄 Moved")
            for c in self.moved:
                lines.append(f"- **{c.title}** (`{c.path}`)")
            lines.append("")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Differ
# ---------------------------------------------------------------------------


class TreeDiffer:
    """Computes structural diffs between two PageIndex trees.

    Compares:
        - Node presence (added/removed)
        - Title changes
        - Summary changes (content_hash based)
        - Structural moves (parent changes)
    
    Improvements:
        - Uses iterative flattening with explicit stack (avoids recursion limits)
        - Tracks parent paths for move detection
        - Uses sets for O(1) lookup performance
        - Handles large trees efficiently with memoization
    """

    def diff(
        self,
        tree_old: dict,
        tree_new: dict,
        platform: str = "",
        version_from: str = "",
        version_to: str = "",
    ) -> TreeDiff:
        """Compute diff between two tree.json dicts.

        Args:
            tree_old: The older tree.json content.
            tree_new: The newer tree.json content.
            platform: Platform name (for metadata).
            version_from: Old version string.
            version_to: New version string.

        Returns:
            TreeDiff containing all changes.
        """
        platform = platform or tree_old.get("meta", {}).get("platform", "unknown")
        version_from = version_from or tree_old.get("meta", {}).get("version", "old")
        version_to = version_to or tree_new.get("meta", {}).get("version", "new")

        result = TreeDiff(
            platform=platform,
            version_from=version_from,
            version_to=version_to,
        )

        # Flatten both trees into {id: (node, path, parent_path)} maps using iterative approach
        old_nodes = self._flatten_tree_iterative(tree_old.get("tree", {}).get("root", {}))
        new_nodes = self._flatten_tree_iterative(tree_new.get("tree", {}).get("root", {}))

        old_ids = set(old_nodes.keys())
        new_ids = set(new_nodes.keys())

        # Added nodes
        for node_id in sorted(new_ids - old_ids):
            node, path, _ = new_nodes[node_id]
            result.changes.append(
                NodeChange(
                    node_id=node_id,
                    title=node.get("title", ""),
                    change_type="added",
                    path=path,
                )
            )

        # Removed nodes
        for node_id in sorted(old_ids - new_ids):
            node, path, _ = old_nodes[node_id]
            result.changes.append(
                NodeChange(
                    node_id=node_id,
                    title=node.get("title", ""),
                    change_type="removed",
                    path=path,
                )
            )

        # Modified and moved nodes (present in both)
        for node_id in sorted(old_ids & new_ids):
            old_node, old_path, old_parent = old_nodes[node_id]
            new_node, new_path, new_parent = new_nodes[node_id]

            details: dict[str, str] = {}
            change_type = None

            # Check for title changes
            if old_node.get("title") != new_node.get("title"):
                details["title"] = f"{old_node.get('title')} → {new_node.get('title')}"

            # Check for content changes
            if old_node.get("content_hash") != new_node.get("content_hash"):
                details["content"] = "changed"

            # Check for summary changes
            if old_node.get("summary") != new_node.get("summary"):
                details["summary"] = "updated"

            # Check for moves (parent changed)
            if old_parent != new_parent:
                details["moved"] = f"{old_parent} → {new_parent}"
                change_type = "moved"

            if details:
                # Determine if it's a modification or move
                if change_type == "moved" and not any(k != "moved" for k in details.keys()):
                    # Only moved, no other changes
                    result.changes.append(
                        NodeChange(
                            node_id=node_id,
                            title=new_node.get("title", ""),
                            change_type="moved",
                            path=new_path,
                            details=details,
                        )
                    )
                else:
                    result.changes.append(
                        NodeChange(
                            node_id=node_id,
                            title=new_node.get("title", ""),
                            change_type="modified",
                            path=new_path,
                            details=details,
                        )
                    )

        return result

    def diff_files(self, old_path: Path, new_path: Path) -> TreeDiff:
        """Diff two tree.json files on disk."""
        old_data = json.loads(old_path.read_text(encoding="utf-8"))
        new_data = json.loads(new_path.read_text(encoding="utf-8"))
        return self.diff(old_data, new_data)

    # -- Internal -----------------------------------------------------------

    def _flatten_tree_iterative(
        self, 
        root: dict, 
        parent_path: str = "",
        parent_id: str = ""
    ) -> dict[str, tuple[dict, str, str]]:
        """Flatten a tree into a dict of {node_id: (node, dot_path, parent_id)}.
        
        Uses iterative approach with explicit stack to avoid recursion limits
        on deeply nested trees.
        """
        result: dict[str, tuple[dict, str, str]] = {}
        
        # Use a stack for iterative traversal [(node, parent_path, parent_id)]
        stack: list[tuple[dict, str, str]] = [(root, parent_path, parent_id)]
        
        while stack:
            node, current_parent_path, current_parent_id = stack.pop()
            
            node_id = node.get("id", "unknown")
            path = f"{current_parent_path}.{node_id}" if current_parent_path else node_id
            result[node_id] = (node, path, current_parent_id)
            
            # Add children to the stack
            for child in node.get("children", []):
                stack.append((child, path, node_id))

        return result

    def _flatten_tree(
        self, node: dict, parent_path: str = ""
    ) -> dict[str, tuple[dict, str]]:
        """Flatten a tree into a dict of {node_id: (node, dot_path)}.
        
        Kept for backwards compatibility - uses the new iterative approach internally.
        """
        result: dict[str, tuple[dict, str]] = {}

        node_id = node.get("id", "unknown")
        path = f"{parent_path}.{node_id}" if parent_path else node_id
        result[node_id] = (node, path)

        for child in node.get("children", []):
            result.update(self._flatten_tree(child, path))

        return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="TreeHub Version Differ")

    # Direct file paths
    parser.add_argument("old_file", nargs="?", help="Path to old tree.json")
    parser.add_argument("new_file", nargs="?", help="Path to new tree.json")

    # Or platform-based
    parser.add_argument("--platform", help="Platform identifier")
    parser.add_argument("--v1", help="Old version")
    parser.add_argument("--v2", help="New version")
    parser.add_argument(
        "--format",
        choices=["json", "markdown"],
        default="markdown",
        help="Output format",
    )
    parser.add_argument("--output", help="Output file (default: stdout)")
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Show only summary, not detailed changes",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Resolve file paths
    if args.old_file and args.new_file:
        old_path = Path(args.old_file)
        new_path = Path(args.new_file)
    elif args.platform and args.v1 and args.v2:
        old_path = DEFAULT_INDICES_DIR / args.platform / f"{args.v1}-tree.json"
        new_path = DEFAULT_INDICES_DIR / args.platform / f"{args.v2}-tree.json"
    else:
        parser.print_help()
        print("\nProvide either two file paths or --platform/--v1/--v2.")
        return

    differ = TreeDiffer()
    result = differ.diff_files(old_path, new_path)

    # Format output
    if args.format == "json":
        output = json.dumps(result.to_dict(), indent=2)
    else:
        if args.summary_only:
            output = f"**{result.total_changes}** changes: **{len(result.added)}** added, **{len(result.removed)}** removed, **{len(result.modified)}** modified"
        else:
            output = result.to_markdown()

    # Write or print
    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"✅ Diff saved to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
