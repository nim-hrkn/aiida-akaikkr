# Copyright (c) 2022-2026 Hiori Kino.
# Distributed under the terms of the Apache License, Version 2.0.
"""`akaikkr-aiida`: command line front end built from cli/spec.py.

With --json, stdout carries exactly one JSON object (also on failure); everything that
libraries print goes to stderr.
"""
import argparse
import contextlib
import importlib
import json
import sys
import traceback

from .spec import KINDS, SUBCOMMANDS, option_flag

_TYPES = {"int": int, "float": float, "str": str}


def _add_options(parser, options):
    for name, (typ, required, help_) in options.items():
        flag = option_flag(name)
        if typ == "bool":
            parser.add_argument(flag, dest=name, action="store_true", help=help_)
        else:
            parser.add_argument(flag, dest=name, type=_TYPES[typ], required=required, default=None, help=help_)


def build_parser():
    parser = argparse.ArgumentParser(prog="akaikkr-aiida", description="AkaiKKR through AiiDA")
    parser.add_argument("--json", action="store_true", help="print one JSON object on stdout")
    parser.add_argument("--profile", default=None, help="AiiDA profile (default: the default profile)")
    parser.add_argument("--caller", default="cli", help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command", required=True)
    for kind in KINDS:
        for name, s in SUBCOMMANDS.items():
            if s["kind"] != kind:
                continue
            p = sub.add_parser(name, help=f"[{kind}] {s['help']}")
            _add_options(p, s["options"])
    return parser


def _load_impl(impl):
    module, func = impl.split(":")
    return getattr(importlib.import_module(module), func)


def run_subcommand(command, kwargs, profile=None, caller="cli"):
    """load the profile, run the implementation, return the result dict (raises on failure)."""
    from aiida import load_profile

    load_profile(profile, allow_switch=True)
    spec = SUBCOMMANDS[command]
    func = _load_impl(spec["impl"])
    if spec["kind"] in ("submit", "control"):
        kwargs = dict(kwargs, caller=caller)
    result = func(**kwargs)
    return result if isinstance(result, dict) else {"result": result}


def _ensure_env_bin_on_path():
    """verdi (daemon control) and dot (graphviz) live next to this interpreter; MCP hosts may not have them on PATH."""
    import os

    bindir = os.path.dirname(sys.executable)
    parts = os.environ.get("PATH", "").split(os.pathsep)
    if bindir not in parts:
        os.environ["PATH"] = os.pathsep.join([bindir] + parts)


def main(argv=None):
    _ensure_env_bin_on_path()
    args = build_parser().parse_args(argv)
    spec = SUBCOMMANDS[args.command]
    kwargs = {name: getattr(args, name) for name in spec["options"]}
    real_stdout = sys.stdout
    try:
        with contextlib.redirect_stdout(sys.stderr):
            result = run_subcommand(args.command, kwargs, profile=args.profile, caller=args.caller)
        out, code = {"ok": True, "command": args.command, **result}, 0
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "command": args.command, "error": f"{type(exc).__name__}: {exc}",
               "hint": getattr(exc, "hint", None)}
        code = 1
        if not args.json:
            traceback.print_exc()
    text = json.dumps(out, default=str) if args.json else json.dumps(out, indent=1, default=str, ensure_ascii=False)
    print(text, file=real_stdout)
    return code


if __name__ == "__main__":
    sys.exit(main())
