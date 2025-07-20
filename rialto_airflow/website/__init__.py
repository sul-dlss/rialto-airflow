"""
This module builds a static website for the current database using the 11ty
static site builder and the https://github.com/sul-dlss-labs/rialto-site
repository.
"""

from .builder import build

__all__ = ["build"]
