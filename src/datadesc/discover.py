from pathlib import Path

SUPPORTED_EXTS = {".csv", ".xlsx", ".xls"}


def discover_sources(inputs, log):
    sources = []
    for root in inputs:
        root = Path(root).expanduser()

        if root.is_file():
            if root.suffix.lower() in SUPPORTED_EXTS:
                sources.append(root)
            else:
                log.warning("Skipping unsupported file: %s", root)
            continue

        if not root.exists():
            log.warning("Input path does not exist: %s", root)
            continue

        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
                sources.append(p)

    sources = sorted(set(sources), key=lambda x: str(x))
    log.info("Discovered %d file(s)", len(sources))
    return sources
