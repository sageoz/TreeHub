
"""
TreeHub Indexer — Builds PageIndex trees from crawled documentation files.

Usage:
    python scripts/indexer.py --platform supabase --version v1
"""

from __future__ import annotations
from dotenv import load_dotenv
import argparse
import hashlib
import json
import logging
import re
# from openai import OpenAI
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import time

logger = logging.getLogger(__name__)

DEFAULT_INDICES_DIR = Path("indices")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

@dataclass
class IndexerConfig:
    indices_dir: Path = DEFAULT_INDICES_DIR
    indexer_version: str = "3.0.0"


# ---------------------------------------------------------------------------
# Tree Node
# ---------------------------------------------------------------------------


@dataclass
class TreeNode:
    id: str
    title: str
    summary: str
    content_hash: str
    children: list["TreeNode"]

    relationships: dict | None = None

    def to_dict(self):

        node = {
            "id": self.id,
            "title": self.title,
            "summary": self.summary,
            "content_hash": self.content_hash,
            "children": [c.to_dict() for c in self.children],
        }

        if self.relationships:
            node["relationships"] = self.relationships

        return node


# ---------------------------------------------------------------------------
# Index Builder
# ---------------------------------------------------------------------------


class PageIndexBuilder:

    def __init__(self, config: IndexerConfig | None = None):

        self.config = config or IndexerConfig()
        # self.client = OpenAI(
        #     base_url="https://openrouter.ai/api/v1",
        #     api_key=os.getenv("OPENROUTER_API_KEY"),
        # )
        # self.last_request_time = 0
        # self.min_request_interval = 1.0 

    # -----------------------------------------------------------------------
    # Load Docs
    # -----------------------------------------------------------------------

    def load_docs(self, platform: str):

        docs_dir = self.config.indices_dir / platform

        if not docs_dir.exists():
            raise FileNotFoundError(f"No crawled docs found for {platform}")

        files = sorted(docs_dir.glob("*.txt"))

        docs = []

        for file in files:

            docs.append(
                {
                    "name": file.name,
                    "content": file.read_text(encoding="utf-8"),
                }
            )

        logger.info("Loaded %d documentation files", len(docs))

        return docs

    # -----------------------------------------------------------------------
    # Parse Docs
    # -----------------------------------------------------------------------

    def parse_all_docs(self, docs):

        sections = []

        for doc in docs:

            parsed = self._parse_sections(doc["content"])

            for s in parsed:
                s["source"] = doc["name"]

            sections.extend(parsed)

        logger.info("Parsed %d sections", len(sections))

        return sections
    
    # -----------------------------------------------------------------------
    # Rate Limit
    # ------------------------------------------------------------------------
    # def _respect_rate_limit(self):

    #     now = time.time()

    #     elapsed = now - self.last_request_time

    #     if elapsed < self.min_request_interval:
    #         time.sleep(self.min_request_interval - elapsed)

    #     self.last_request_time = time.time()

    # -----------------------------------------------------------------------
    # Section Parser
    # -----------------------------------------------------------------------
    
    
    def _parse_sections(self, content: str) -> list[dict]:

        if not content:
            return []

        sections = []
        current = None

        lines = content.splitlines()

        for line in lines:

            line = line.strip()

            # ------------------------------------------------
            # 1️⃣ Markdown Headings
            # ------------------------------------------------

            heading = re.match(r"^(#{1,6})\s+(.*)", line)

            if heading:

                if current:
                    sections.append(current)

                current = {
                    "title": heading.group(2).strip(),
                    "level": len(heading.group(1)),
                    "body": "",
                }

                continue

            # ------------------------------------------------
            # 2️⃣ Markdown Links
            # ------------------------------------------------

            link = re.match(r"-\s*\[(.*?)\]\((.*?)\)", line)

            if link:

                if current:
                    sections.append(current)

                title = link.group(1).strip()
                url = link.group(2).strip()

                current = {
                    "title": title,
                    "level": 2,
                    "body": f"Documentation page: {title}",
                    "url": url,
                }

                continue

            # ------------------------------------------------
            # 3️⃣ Bullet Lists
            # ------------------------------------------------

            bullet = re.match(r"^[-*]\s+(.*)", line)

            if bullet:

                text = bullet.group(1)

                if current:
                    current["body"] += text + "\n"

                continue

            # ------------------------------------------------
            # 4️⃣ Normal text
            # ------------------------------------------------

            if current:
                current["body"] += line + "\n"

        if current:
            sections.append(current)

        if not sections:

            sections.append(
                {
                    "title": "Overview",
                    "level": 1,
                    "body": content,
                }
            )

        return sections



    # -----------------------------------------------------------------------
    # Summary Generator
    # -----------------------------------------------------------------------

    def generate_summary(self, title: str, body: str):

        if not body:
            return title

        first_line = body.strip().split("\n")[0]

        summary = first_line[:160]

        if len(summary) < 25:
            summary = f"Documentation about {title}"

        return summary

    

    # def batch_summarize(self, sections, batch_size=1000):

        summaries = []

        for i in range(0, len(sections), batch_size):
            print(len(sections))
            batch = sections[i:i + batch_size]
            

            payload = []

            for s in batch:
                payload.append({
                    "title": s["title"],
                    "content": s["body"][:350]
                })

            prompt = f"""
Summarize each documentation section in ONE short sentence.

Return ONLY a valid JSON array of summaries.

Rules:
- No explanations
- No markdown
- No text outside JSON
- Output must start with [

Sections:
{json.dumps(payload)}
"""

            for attempt in range(2):

                try:
                    self._respect_rate_limit()
                    response = self.client.chat.completions.create(
                        model="z-ai/glm-4.5-air:free",
                        messages=[
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.2
                    )

                    result = response.choices[0].message.content.strip()

                    # extract JSON safely
                    json_match = re.search(r"\[.*\]", result, re.DOTALL)

                    if not json_match:
                        raise ValueError("No JSON array found in LLM response")

                    batch_summaries = json.loads(json_match.group())

                    if len(batch_summaries) != len(batch):
                        raise ValueError("LLM returned incorrect number of summaries")

                    summaries.extend(batch_summaries)

                    break

                except Exception as e:

                    logger.warning("Batch summarization failed: %s", e)

                    if attempt < 1:
                        time.sleep(1** attempt)
                    else:
                        # fallback summaries
                        for s in batch:
                            summaries.append(
                                self.generate_summary(s["title"], s["body"])
                            )

        return summaries

    # -----------------------------------------------------------------------
    # Tree Builder
    # -----------------------------------------------------------------------

    def build_tree(self, platform, sections):

        root = TreeNode(
            id="root",
            title=f"{platform.title()} Documentation",
            summary=f"Documentation index for {platform}",
            content_hash="",
            children=[],
        )

        stack = [(0, root)]

        for section in sections:

            source = section.get("source", "doc")

            safe_title = section["title"].lower().replace(" ", "-")
            safe_title = re.sub(r"[^a-z0-9\-]", "", safe_title)

            node_id = f"{source}-{safe_title}"

            body = section["body"].strip()

            # Use local summary generator instead of LLM
            summary = self.generate_summary(section["title"], body)

            content_hash = hashlib.sha256(body.encode()).hexdigest()[:24]

            node = TreeNode(
                id=node_id,
                title=section["title"],
                summary=summary,
                content_hash=f"sha256:{content_hash}",
                children=[],
            )

            level = section["level"]

            while stack and stack[-1][0] >= level:
                stack.pop()

            parent = stack[-1][1]

            parent.children.append(node)

            node.relationships = {
                "parent": parent.id,
                "related": [],
                "next_page": None,
            }

            stack.append((level, node))

        return root

    # -----------------------------------------------------------------------
    # Build + Save
    # -----------------------------------------------------------------------

   
    def build_and_save(self, platform: str, version: str):

        docs = self.load_docs(platform)

        out_dir = self.config.indices_dir / platform / "trees"
        out_dir.mkdir(parents=True, exist_ok=True)

        # -----------------------------
        # Parse ALL docs first
        # -----------------------------

        sections = self.parse_all_docs(docs)

        logger.info("Total sections: %d", len(sections))
        sections_by_doc = {}

       

        

        for s in sections:

            source = s.get("source", "doc")

            sections_by_doc.setdefault(source, []).append(s)

        tree_files = []

        # -----------------------------
        # Build trees per document
        # -----------------------------

        for doc in docs:

            name = doc["name"]

            logger.info("Building tree for %s", name)

            doc_sections = sections_by_doc.get(name, [])

            root = self.build_tree(platform, doc_sections)

            tree_json = json.dumps(root.to_dict(), indent=2)

            tree_hash = hashlib.sha256(tree_json.encode()).hexdigest()

            tree_data = {
                "meta": {
                    "platform": platform,
                    "version": version,
                    "source_file": name,
                    "indexed_at": datetime.now(timezone.utc).isoformat(),
                    "tree_hash": f"sha256:{tree_hash}",
                    "pages_count": self.count_nodes(root),
                    "indexer_version": self.config.indexer_version,
                },
                "tree": {"root": root.to_dict()},
            }

            filename = name.replace(".txt", "-tree.json")

            path = out_dir / filename

            path.write_text(json.dumps(tree_data, indent=2), encoding="utf-8")

            tree_files.append(filename)

        manifest = {
            "platform": platform,
            "version": version,
            "trees": tree_files,
        }

        manifest_path = self.config.indices_dir / platform / f"{version}-manifest.json"

        manifest_path.write_text(json.dumps(manifest, indent=2))

        return manifest_path
    
    def count_nodes(self, node):
        count = 1
        for child in node.children:
            count += self.count_nodes(child)
        return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():

    parser = argparse.ArgumentParser(description="TreeHub Index Builder")

    parser.add_argument("--platform", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", default=str(DEFAULT_INDICES_DIR))

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    config = IndexerConfig(indices_dir=Path(args.output))

    builder = PageIndexBuilder(config)

    manifest_path = builder.build_and_save(
        args.platform,
        args.version,
    )

    print(f"Indexed {args.platform}@{args.version}")
    print(f"Indexed {args.platform}@{args.version}")
    print("Manifest:", manifest_path)


if __name__ == "__main__":
    main()

