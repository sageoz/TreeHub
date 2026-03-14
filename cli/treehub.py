"""
TreeHub CLI — Local index management for PageIndex trees.

Usage:
    treehub list                          List available platforms
    treehub versions <platform>           List versions for a platform
    treehub pull <platform>@<version>     Download an index
    treehub verify <platform>@<version>   Verify integrity
    treehub preview <platform>@<version>  Preview tree structure
    treehub diff <platform>@<v1> <v2>     Compare two versions
    treehub export <platform>@<version>   Export to markdown
    treehub cache ls|clear|prune          Manage local cache

Install:
    pip install -e ./cli
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

console = Console()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CACHE_DIR = Path.home() / ".treehub" / "cache"
DEFAULT_REGISTRY_URL = "https://raw.githubusercontent.com/treehub/indices/main/registry.json"
DEFAULT_INDICES_DIR = Path(__file__).resolve().parent.parent / "indices"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_platform_version(spec: str) -> tuple[str, str]:
    """Parse 'platform@version' into (platform, version).
    
    Args:
        spec: A string in format 'platform@version' or just 'platform'.
        
    Returns:
        Tuple of (platform, version) where version defaults to 'latest'.
        
    Raises:
        ValueError: If the platform name is invalid.
    """
    if not spec or not isinstance(spec, str):
        raise ValueError("Platform/version spec cannot be empty")
    
    if "@" in spec:
        platform, version = spec.split("@", 1)
    else:
        platform, version = spec, "latest"
    
    # Validate platform name (lowercase alphanumeric with hyphens)
    if platform and not platform.replace("-", "").isalnum():
        raise ValueError(
            f"Invalid platform name: '{platform}'. "
            "Must be lowercase alphanumeric with optional hyphens."
        )
    
    # Validate version format (basic check)
    if version and not version.replace(".", "").replace("-", "").isalnum():
        raise ValueError(
            f"Invalid version format: '{version}'. "
            "Must be alphanumeric with optional dots and hyphens."
        )
    
    return platform, version


def _get_local_tree_path(platform: str, version: str) -> Path | None:
    """Look up a local tree file."""
    platform_dir = DEFAULT_INDICES_DIR / platform
    if not platform_dir.is_dir():
        return None

    if version == "latest":
        latest = platform_dir / "latest.json"
        if latest.exists():
            return latest
        tree_files = sorted(platform_dir.glob("*-tree.json"), reverse=True)
        return tree_files[0] if tree_files else None

    tree_file = platform_dir / f"{version}-tree.json"
    return tree_file if tree_file.exists() else None


def _load_local_tree(platform: str, version: str) -> dict | None:
    """Load a local tree.json file."""
    path = _get_local_tree_path(platform, version)
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _print_tree_node(tree_widget: Tree, node: dict, max_depth: int, depth: int = 0) -> None:
    """Recursively print tree nodes using Rich."""
    if depth > max_depth:
        return

    title = node.get("title", "Untitled")
    node_id = node.get("id", "")
    summary = node.get("summary", "")

    label = f"[bold]{title}[/bold] [dim]({node_id})[/dim]"
    if summary:
        label += f"\n  [dim italic]{summary[:80]}{'...' if len(summary) > 80 else ''}[/dim italic]"

    branch = tree_widget.add(label)

    for child in node.get("children", []):
        _print_tree_node(branch, child, max_depth, depth + 1)


# ---------------------------------------------------------------------------
# CLI App
# ---------------------------------------------------------------------------


@click.group()
@click.version_option("0.1.0", prog_name="treehub")
def cli() -> None:
    """🌳 TreeHub — The Wikipedia of AI Indices.

    Manage pre-built, versioned PageIndex trees for popular developer platforms.
    """
    pass


# -- list -------------------------------------------------------------------

@cli.command("list")
def list_platforms() -> None:
    """List all available platforms."""
    table = Table(title="📦 Available Platforms", show_header=True)
    table.add_column("Platform", style="bold cyan")
    table.add_column("Versions", justify="right")
    table.add_column("Latest", style="green")

    for platform_dir in sorted(DEFAULT_INDICES_DIR.iterdir()):
        if not platform_dir.is_dir() or platform_dir.name.startswith("_"):
            continue

        tree_files = sorted(platform_dir.glob("*-tree.json"))
        versions = [f.stem.replace("-tree", "") for f in tree_files]

        table.add_row(
            platform_dir.name,
            str(len(versions)),
            versions[-1] if versions else "-",
        )

    console.print(table)


# -- versions ---------------------------------------------------------------

@cli.command()
@click.argument("platform")
def versions(platform: str) -> None:
    """List available versions for a PLATFORM."""
    platform_dir = DEFAULT_INDICES_DIR / platform
    if not platform_dir.is_dir():
        console.print(f"[red]Platform not found:[/red] {platform}")
        raise SystemExit(1)

    table = Table(title=f"📋 Versions for {platform}", show_header=True)
    table.add_column("Version", style="bold")
    table.add_column("Date")
    table.add_column("Pages", justify="right")

    for tree_file in sorted(platform_dir.glob("*-tree.json")):
        version = tree_file.stem.replace("-tree", "")
        manifest_file = platform_dir / f"{version}-manifest.json"

        date = "-"
        pages = "-"
        if manifest_file.exists():
            manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
            date = manifest.get("snapshot_date", "-")
            pages = str(manifest.get("stats", {}).get("pages_count", "-"))

        table.add_row(version, date, pages)

    console.print(table)


# -- pull -------------------------------------------------------------------

@cli.command()
@click.argument("spec")
def pull(spec: str) -> None:
    """Pull an index: SPEC is platform@version (e.g. supabase@v2.1.0)."""
    platform, version = _parse_platform_version(spec)

    tree_path = _get_local_tree_path(platform, version)
    if tree_path is None:
        console.print(f"[red]Not found:[/red] {platform}@{version}")
        console.print("[dim]This platform/version is not available locally.")
        console.print("Check 'treehub list' for available platforms.[/dim]")
        raise SystemExit(1)

    # Copy to cache
    cache_dest = CACHE_DIR / platform
    cache_dest.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dest / tree_path.name
    shutil.copy2(tree_path, cache_file)

    tree = json.loads(tree_path.read_text(encoding="utf-8"))
    meta = tree.get("meta", {})

    console.print(
        Panel(
            f"[bold green]✅ Pulled {platform}@{version}[/bold green]\n\n"
            f"  Pages:   {meta.get('pages_count', '?')}\n"
            f"  Indexed: {meta.get('indexed_at', '?')}\n"
            f"  Cached:  {cache_file}",
            title="treehub pull",
        )
    )


# -- verify -----------------------------------------------------------------

@cli.command()
@click.argument("spec")
def verify(spec: str) -> None:
    """Verify integrity of an index: SPEC is platform@version."""
    platform, version = _parse_platform_version(spec)

    tree_path = _get_local_tree_path(platform, version)
    if tree_path is None:
        console.print(f"[red]Not found:[/red] {platform}@{version}")
        raise SystemExit(1)

    # Compute hash
    content = tree_path.read_bytes()
    actual_hash = f"sha256:{hashlib.sha256(content).hexdigest()}"

    # Check against manifest
    platform_dir = DEFAULT_INDICES_DIR / platform
    manifest_file = platform_dir / f"{version}-manifest.json"

    if manifest_file.exists():
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        expected_hash = manifest.get("files", {}).get("tree", {}).get("hash")

        if expected_hash and actual_hash == expected_hash:
            console.print(f"[bold green]✅ Verified:[/bold green] {platform}@{version}")
            console.print(f"   Hash: {actual_hash[:40]}...")
        else:
            console.print(f"[bold red]❌ Hash mismatch:[/bold red] {platform}@{version}")
            console.print(f"   Expected: {expected_hash}")
            console.print(f"   Actual:   {actual_hash}")
            raise SystemExit(1)
    else:
        console.print(f"[yellow]⚠️  No manifest found for {platform}@{version}[/yellow]")
        console.print(f"   File hash: {actual_hash[:40]}...")


# -- preview ----------------------------------------------------------------

@cli.command()
@click.argument("spec")
@click.option("--max-depth", default=3, help="Max depth to display.")
def preview(spec: str, max_depth: int) -> None:
    """Preview tree structure: SPEC is platform@version."""
    platform, version = _parse_platform_version(spec)

    tree_data = _load_local_tree(platform, version)
    if tree_data is None:
        console.print(f"[red]Not found:[/red] {platform}@{version}")
        raise SystemExit(1)

    root = tree_data.get("tree", {}).get("root", {})
    meta = tree_data.get("meta", {})

    tree_widget = Tree(
        f"🌳 [bold]{meta.get('platform', platform)}[/bold] "
        f"[dim]{meta.get('version', version)}[/dim]"
    )
    _print_tree_node(tree_widget, root, max_depth)
    console.print(tree_widget)


# -- status -----------------------------------------------------------------

@cli.command()
@click.argument("spec")
def status(spec: str) -> None:
    """Compare local vs remote index: SPEC is platform@version."""
    platform, version = _parse_platform_version(spec)

    tree_data = _load_local_tree(platform, version)
    if tree_data is None:
        console.print(f"[red]Not found locally:[/red] {platform}@{version}")
        raise SystemExit(1)

    meta = tree_data.get("meta", {})

    console.print(
        Panel(
            f"[bold]{platform}@{version}[/bold]\n\n"
            f"  Pages:     {meta.get('pages_count', '?')}\n"
            f"  Indexed:   {meta.get('indexed_at', '?')}\n"
            f"  Source:    {meta.get('source_url', '?')}\n"
            f"  Hash:      {meta.get('tree_hash', '?')[:40]}...\n",
            title="📊 Index Status",
        )
    )


# -- diff -------------------------------------------------------------------

@cli.command("diff")
@click.argument("spec")
@click.argument("v2")
def diff_versions(spec: str, v2: str) -> None:
    """Diff two versions: SPEC is platform@v1, V2 is the other version."""
    import sys
    sys.path.insert(0, str(DEFAULT_INDICES_DIR.parent))

    from scripts.differ import TreeDiffer

    platform, v1 = _parse_platform_version(spec)

    tree_old = _load_local_tree(platform, v1)
    tree_new = _load_local_tree(platform, v2)

    if tree_old is None:
        console.print(f"[red]Not found:[/red] {platform}@{v1}")
        raise SystemExit(1)
    if tree_new is None:
        console.print(f"[red]Not found:[/red] {platform}@{v2}")
        raise SystemExit(1)

    differ = TreeDiffer()
    result = differ.diff(tree_old, tree_new, platform, v1, v2)

    console.print(result.to_markdown())


# -- export -----------------------------------------------------------------

@cli.command()
@click.argument("spec")
@click.option("--format", "fmt", type=click.Choice(["markdown", "json"]), default="markdown")
@click.option("--output", "-o", default=None, help="Output file path.")
def export(spec: str, fmt: str, output: str | None) -> None:
    """Export an index to another format: SPEC is platform@version."""
    platform, version = _parse_platform_version(spec)

    tree_data = _load_local_tree(platform, version)
    if tree_data is None:
        console.print(f"[red]Not found:[/red] {platform}@{version}")
        raise SystemExit(1)

    if fmt == "json":
        content = json.dumps(tree_data, indent=2)
    else:
        content = _tree_to_markdown(tree_data)

    if output:
        Path(output).write_text(content, encoding="utf-8")
        console.print(f"[green]✅ Exported to {output}[/green]")
    else:
        console.print(content)


def _tree_to_markdown(tree_data: dict) -> str:
    """Convert a tree to markdown format."""
    meta = tree_data.get("meta", {})
    lines = [
        f"# {meta.get('platform', 'Unknown')} Documentation Index",
        f"",
        f"**Version:** {meta.get('version', '?')}  ",
        f"**Indexed:** {meta.get('indexed_at', '?')}  ",
        f"**Pages:** {meta.get('pages_count', '?')}  ",
        f"",
    ]

    root = tree_data.get("tree", {}).get("root", {})
    _node_to_markdown(root, lines, depth=0)
    return "\n".join(lines)


def _node_to_markdown(node: dict, lines: list[str], depth: int) -> None:
    """Recursively convert a node to markdown."""
    prefix = "#" * min(depth + 2, 6)
    title = node.get("title", "Untitled")
    summary = node.get("summary", "")

    lines.append(f"{prefix} {title}")
    if summary:
        lines.append(f"")
        lines.append(summary)
    lines.append("")

    for child in node.get("children", []):
        _node_to_markdown(child, lines, depth + 1)


# -- cache ------------------------------------------------------------------

@cli.group()
def cache() -> None:
    """Manage local cache."""
    pass


@cache.command("ls")
def cache_ls() -> None:
    """List cached indices."""
    if not CACHE_DIR.exists():
        console.print("[dim]Cache is empty.[/dim]")
        return

    table = Table(title="🗂️  Local Cache", show_header=True)
    table.add_column("Platform")
    table.add_column("Files", justify="right")
    table.add_column("Size", justify="right")

    for platform_dir in sorted(CACHE_DIR.iterdir()):
        if not platform_dir.is_dir():
            continue
        files = list(platform_dir.glob("*.json"))
        total_size = sum(f.stat().st_size for f in files)
        size_str = f"{total_size / 1024:.1f} KB" if total_size < 1_000_000 else f"{total_size / 1_000_000:.1f} MB"
        table.add_row(platform_dir.name, str(len(files)), size_str)

    console.print(table)


@cache.command("clear")
@click.confirmation_option(prompt="Are you sure you want to clear the cache?")
def cache_clear() -> None:
    """Clear all cached indices."""
    if CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)
        console.print("[green]✅ Cache cleared.[/green]")
    else:
        console.print("[dim]Cache is already empty.[/dim]")


@cache.command("prune")
@click.option("--older-than", default="30d", help="Remove entries older than (e.g. 30d, 7d).")
def cache_prune(older_than: str) -> None:
    """Remove old cached entries."""
    if not CACHE_DIR.exists():
        console.print("[dim]Cache is empty.[/dim]")
        return

    # Parse duration
    days = int(older_than.rstrip("d"))
    cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)

    removed = 0
    for platform_dir in CACHE_DIR.iterdir():
        if not platform_dir.is_dir():
            continue
        for f in platform_dir.glob("*.json"):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        # Remove empty dirs
        if not any(platform_dir.iterdir()):
            platform_dir.rmdir()

    console.print(f"[green]✅ Pruned {removed} entries older than {older_than}.[/green]")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cli()
