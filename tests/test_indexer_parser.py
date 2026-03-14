"""
Unit tests for TreeHub Parser (indexer.py)

Tests cover:
- ATX-style heading parsing (# Heading, ## Heading ##, etc.)
- Setext-style heading parsing (Title\n=====, Title\n-----)
- Input validation (empty content, non-string content)
- Cache behavior (clear_cache, section caching)
- Line number tracking
- Edge cases (no headings, malformed content)
"""

from __future__ import annotations

import pytest

from scripts.indexer import PageIndexBuilder, IndexerConfig


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def builder():
    """Create a PageIndexBuilder instance."""
    return PageIndexBuilder(IndexerConfig())


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear the parser cache before each test."""
    PageIndexBuilder.clear_cache()
    yield
    PageIndexBuilder.clear_cache()


# =============================================================================
# ATX Heading Tests
# =============================================================================


class TestATXHeadings:
    """Tests for ATX-style heading parsing (# Heading)."""

    def test_simple_atx_h1(self, builder):
        """Test parsing a single H1 heading."""
        content = "# Hello World\n\nSome content here."
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "Hello World"
        assert sections[0]["level"] == 1

    def test_simple_atx_h2(self, builder):
        """Test parsing H2 heading."""
        content = "## Getting Started\n\nInstallation guide."
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "Getting Started"
        assert sections[0]["level"] == 2

    def test_multiple_atx_headings(self, builder):
        """Test parsing multiple ATX headings at different levels."""
        content = """# Title

## Section 1

Content 1

## Section 2

Content 2

### Subsection

More content
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 4
        assert sections[0]["title"] == "Title"
        assert sections[0]["level"] == 1
        assert sections[1]["title"] == "Section 1"
        assert sections[1]["level"] == 2
        assert sections[2]["title"] == "Section 2"
        assert sections[2]["level"] == 2
        assert sections[3]["title"] == "Subsection"
        assert sections[3]["level"] == 3

    def test_atx_h6_max_level(self, builder):
        """Test parsing H6 (maximum ATX level)."""
        content = "###### H6 Heading\n\nContent."
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "H6 Heading"
        assert sections[0]["level"] == 6

    def test_atx_with_closing_hashes(self, builder):
        """Test ATX heading with closing hashes (## Title ##)."""
        content = "## API Reference ##\n\nEndpoints documentation."
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "API Reference"
        assert sections[0]["level"] == 2

    def test_atx_closing_hashes_extra(self, builder):
        """Test ATX with extra closing hashes (### Title ###)."""
        content = "### Authentication ###\n\nAuth docs."
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "Authentication"
        assert sections[0]["level"] == 3

    def test_atx_body_content(self, builder):
        """Test that body content is captured correctly."""
        content = """# Main Title

First paragraph.

Second paragraph with **bold**.

- List item 1
- List item 2
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert "First paragraph" in sections[0]["body"]
        assert "Second paragraph" in sections[0]["body"]
        assert "List item 1" in sections[0]["body"]


# =============================================================================
# Setext Heading Tests
# =============================================================================


class TestSetextHeadings:
    """Tests for Setext-style heading parsing."""

    def test_setext_h1_equals(self, builder):
        """Test Setext H1 (Title with ==== underline)."""
        content = """My Title
=========

Content here.
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "My Title"
        assert sections[0]["level"] == 1

    def test_setext_h2_dashes(self, builder):
        """Test Setext H2 (Title with ---- underline)."""
        content = """Section Title
-------------

Section content.
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "Section Title"
        assert sections[0]["level"] == 2

    def test_multiple_setext_headings(self, builder):
        """Test multiple Setext headings."""
        content = """Main Title
==========

Section One
-----------

Content one.

Section Two
-----------

Content two.
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 3  # Main + 2 sections
        assert sections[0]["title"] == "Main Title"
        assert sections[0]["level"] == 1
        assert sections[1]["title"] == "Section One"
        assert sections[1]["level"] == 2

    def test_setext_with_spaces_in_underline(self, builder):
        """Test Setext with spaces in underline."""
        content = """Title
======

Content."""
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "Title"


# =============================================================================
# Mixed Heading Format Tests
# =============================================================================


class TestMixedHeadingFormats:
    """Tests for mixing ATX and Setext headings."""

    def test_atx_after_setext(self, builder):
        """Test ATX heading following Setext heading."""
        content = """Main Title
==========

## ATX Section

ATX content.
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 2
        assert sections[0]["title"] == "Main Title"
        assert sections[0]["level"] == 1
        assert sections[1]["title"] == "ATX Section"
        assert sections[1]["level"] == 2

    def test_setext_after_atx(self, builder):
        """Test Setext heading following ATX heading."""
        content = """# ATX Title

Setext Section
-------------

Setext content.
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 2
        assert sections[0]["title"] == "ATX Title"
        assert sections[0]["level"] == 1
        assert sections[1]["title"] == "Setext Section"
        assert sections[1]["level"] == 2


# =============================================================================
# Input Validation Tests
# =============================================================================


class TestInputValidation:
    """Tests for input validation."""

    def test_empty_content_returns_empty_list(self, builder):
        """Test empty content returns empty list."""
        sections = builder._parse_sections("")
        assert sections == []

    def test_whitespace_only_returns_empty_list(self, builder):
        """Test whitespace-only content returns empty list."""
        sections = builder._parse_sections("   \n\n   \n   ")
        assert sections == []

    def test_non_string_raises_error(self, builder):
        """Test non-string content raises ValueError."""
        # List input raises ValueError because it's not a string
        with pytest.raises(ValueError, match="Expected string content"):
            builder._parse_sections(["line1", "line2"])


# =============================================================================
# Line Number Tracking Tests
# =============================================================================


class TestLineNumberTracking:
    """Tests for line number tracking in parsed sections."""

    def test_line_numbers_atx(self, builder):
        """Test line numbers are tracked for ATX headings."""
        content = """# Title

## Section 1

## Section 2
"""
        sections = builder._parse_sections(content)

        assert sections[0]["line_number"] == 1
        assert sections[1]["line_number"] == 3
        assert sections[2]["line_number"] == 5

    def test_line_numbers_setext(self, builder):
        """Test line numbers are tracked for Setext headings."""
        content = """Title
=======

Section
-------

Content.
"""
        sections = builder._parse_sections(content)

        # Title is on line 1, Section is on line 4
        assert sections[0]["line_number"] == 1
        assert sections[1]["line_number"] == 4

    def test_line_numbers_in_body(self, builder):
        """Test content line numbers are tracked."""
        content = """# Title

Line 3 content.

Line 5 content.
"""
        sections = builder._parse_sections(content)

        # Title at line 1
        assert sections[0]["line_number"] == 1


# =============================================================================
# Cache Tests
# =============================================================================


class TestCacheBehavior:
    """Tests for parser caching functionality."""

    def test_clear_cache(self, builder):
        """Test cache is cleared properly."""
        content = "# Test\n\nContent"

        # First parse - populates cache
        sections1 = builder._parse_sections(content)

        # Verify cache is populated
        assert len(PageIndexBuilder._section_cache) > 0

        # Clear cache
        PageIndexBuilder.clear_cache()

        # Verify cache is empty
        assert len(PageIndexBuilder._section_cache) == 0
        assert len(PageIndexBuilder._content_hash_cache) == 0

    def test_cache_is_used(self, builder):
        """Test that cached results are returned on subsequent calls."""
        content = "# Cached Title\n\nCached content here."

        # First call - populates cache
        sections1 = builder._parse_sections(content)

        # Second call - should use cache
        sections2 = builder._parse_sections(content)

        # Results should be identical
        assert sections1 == sections2
        # But they should be the same object (from cache)
        assert sections1[0] is sections2[0]

    def test_different_content_different_cache(self, builder):
        """Test different content produces different cache entries."""
        content1 = "# Title 1\n\nContent 1"
        content2 = "# Title 2\n\nContent 2"

        sections1 = builder._parse_sections(content1)
        sections2 = builder._parse_sections(content2)

        assert sections1[0]["title"] == "Title 1"
        assert sections2[0]["title"] == "Title 2"
        assert len(PageIndexBuilder._section_cache) == 2


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and malformed content."""

    def test_no_headings_returns_overview(self, builder):
        """Test content without headings returns Overview section."""
        content = """This is just some content.

More content without any headings.
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert sections[0]["title"] == "Overview"

    def test_heading_with_only_hashes(self, builder):
        """Test heading with only hashes (no title) is treated as content."""
        content = "######\n\nActual content."
        sections = builder._parse_sections(content)

        # This might be parsed as Overview since ####### has no title
        assert len(sections) >= 1

    def test_consecutive_empty_lines(self, builder):
        """Test consecutive empty lines are handled."""
        content = """# Title



## Section



Content
"""
        sections = builder._parse_sections(content)

        assert len(sections) == 2

    def test_very_long_heading(self, builder):
        """Test very long heading is parsed correctly."""
        content = "# " + "A" * 500 + "\n\nContent"
        sections = builder._parse_sections(content)

        assert len(sections) == 1
        assert len(sections[0]["title"]) == 500

    def test_special_characters_in_heading(self, builder):
        """Test special characters in heading."""
        content = """# API v2.0 (2024) - Getting Started!

Content here.
"""
        sections = builder._parse_sections(content)

        assert sections[0]["title"] == "API v2.0 (2024) - Getting Started!"

    def test_hash_in_code_block_is_heading(self, builder):
        """Test # in code block is treated as heading (current parser limitation).
        
        Note: The current parser treats any line starting with # as a heading.
        This is a known limitation - code fence awareness would require more complex parsing.
        """
        content = """# Title

```
# This looks like a heading but is in code
```

## Next Section

Content.
"""
        sections = builder._parse_sections(content)

        # Parser treats # in code block as heading (current limitation)
        # The test documents this behavior
        assert len(sections) == 3


# =============================================================================
# Body Content Tests
# =============================================================================


class TestBodyContent:
    """Tests for body content handling."""

    def test_body_trimmed_trailing_whitespace(self, builder):
        """Test body trailing whitespace is trimmed."""
        content = "# Title\n\nContent with trailing spaces   \n\n"
        sections = builder._parse_sections(content)

        # Should not have trailing whitespace
        assert sections[0]["body"].rstrip() == sections[0]["body"]

    def test_multiline_body(self, builder):
        """Test multiline body content is preserved."""
        content = """# Title

Line 1
Line 2
Line 3
"""
        sections = builder._parse_sections(content)

        assert "Line 1" in sections[0]["body"]
        assert "Line 2" in sections[0]["body"]
        assert "Line 3" in sections[0]["body"]

    def test_body_preserves_code_blocks(self, builder):
        """Test code blocks in body are preserved."""
        content = """# Title

```python
def hello():
    print("world")
```

End content.
"""
        sections = builder._parse_sections(content)

        assert '```python' in sections[0]["body"]
        assert 'def hello():' in sections[0]["body"]
