from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

try:
    from dotenv import dotenv_values
except Exception:
    dotenv_values = None


def _load_paths(project_root: Path) -> tuple[Path, Path]:
    output_dir = project_root / "output"
    temp_dir = project_root / "temp"

    env_path = project_root / ".env"
    if dotenv_values and env_path.exists():
        env = dotenv_values(str(env_path))
        output_raw = (env.get("OUTPUT_DIR") or "").strip()
        temp_raw = (env.get("TEMP_DIR") or "").strip()

        if output_raw:
            output_dir = Path(output_raw)
            if not output_dir.is_absolute():
                output_dir = project_root / output_dir

        if temp_raw:
            temp_dir = Path(temp_raw)
            if not temp_dir.is_absolute():
                temp_dir = project_root / temp_dir

    return output_dir, temp_dir


def _rm_path(path: Path, dry_run: bool) -> bool:
    if not path.exists():
        return False

    print(f"- remove: {path}")
    if dry_run:
        return True

    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)
    return True


def _clean_directory_contents(path: Path, dry_run: bool) -> int:
    if not path.exists() or not path.is_dir():
        return 0

    removed = 0
    for item in path.iterdir():
        if item.name == ".gitkeep":
            continue
        if _rm_path(item, dry_run=dry_run):
            removed += 1
    return removed


def build_targets(project_root: Path, include_token: bool) -> tuple[list[Path], Path, Path]:
    output_dir, temp_dir = _load_paths(project_root)

    targets = [
        output_dir / "brotochondria.db",
        output_dir / "brotochondria.log",
        output_dir / "upload_manifest.json",
        output_dir / "exports",
    ]

    if include_token:
        targets.append(project_root / "token.json")

    return targets, output_dir, temp_dir


def _preview_targets(targets: list[Path], temp_dir: Path) -> None:
    print("\nDeletion preview:")
    for target in targets:
        status = "exists" if target.exists() else "missing"
        kind = "dir" if target.is_dir() else "file"
        print(f"  - {target} [{kind}, {status}]")

    if temp_dir.exists() and temp_dir.is_dir():
        temp_items = list(temp_dir.iterdir())
        print(f"  - {temp_dir}/* [directory contents, {len(temp_items)} item(s)]")
        for item in temp_items[:10]:
            kind = "dir" if item.is_dir() else "file"
            print(f"      · {item.name} [{kind}]")
        if len(temp_items) > 10:
            print(f"      · ... and {len(temp_items) - 10} more")
    else:
        print(f"  - {temp_dir}/* [directory contents, missing]")


def run_reset(project_root: Path, dry_run: bool, include_token: bool) -> int:
    targets, output_dir, temp_dir = build_targets(project_root, include_token)

    print("Brotochondria reset")
    print(f"Project root: {project_root}")
    print(f"Output dir:   {output_dir}")
    print(f"Temp dir:     {temp_dir}")
    print(f"Dry run:      {dry_run}")
    print(f"Reset token:  {include_token}")

    changed = 0
    for target in targets:
        if _rm_path(target, dry_run=dry_run):
            changed += 1

    cleaned_temp = _clean_directory_contents(temp_dir, dry_run=dry_run)
    changed += cleaned_temp

    print(f"\nDone. Removed {changed} item(s).")

    if not dry_run:
        (output_dir / "exports").mkdir(parents=True, exist_ok=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        print(f"Ensured: {(output_dir / 'exports')}")
        print(f"Ensured: {temp_dir}")

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean extraction state (DB, exports, temp files) for a fresh run."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting anything.",
    )
    parser.add_argument(
        "--include-token",
        action="store_true",
        help="Also delete token.json so Google OAuth is re-authenticated.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]
    targets, _, temp_dir = build_targets(project_root, args.include_token)

    _preview_targets(targets, temp_dir)

    if not args.dry_run and not args.yes:
        print("\nType RESET to confirm deletion.")
        answer = input("Confirmation: ").strip()
        if answer != "RESET":
            print("Cancelled.")
            return 1

    return run_reset(
        project_root=project_root,
        dry_run=args.dry_run,
        include_token=args.include_token,
    )


if __name__ == "__main__":
    sys.exit(main())
