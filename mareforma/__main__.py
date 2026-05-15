"""Allow `python -m mareforma` to invoke the CLI.

A common Python discovery convention: developers who installed the
package try ``python -m <package>`` before figuring out the
console-script name. Route it to the same Click app the ``mareforma``
console-script uses.
"""

from mareforma.cli import cli

if __name__ == "__main__":
    cli()
