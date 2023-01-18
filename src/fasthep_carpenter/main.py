import os
from pathlib import Path
import rich
import sys
import typer

from .utils import mkdir_p

app = typer.Typer()


@app.command()
def main(
    dataset_cfg: Path = typer.Argument(
        ..., help="Dataset config to run over", exists=True, readable=True,
        resolve_path=True,
    ),
    sequence_cfg: Path = typer.Argument(
        ..., help="Config for how to process dataset", exists=True, readable=True,
    ),
    output_dir: str = typer.Option(
        "output", "--outdir", "-o", help="Where to save the results"
    ),
    processing_backend: str = typer.Option(
        "multiprocessing", "--backend", "-b", help="Backend to use for processing"
    ),
    store_bookkeeping: bool = typer.Option(
        True, "--store-bookkeeping", "-s", help="Store bookkeeping information"
    ),
):
    try:
        import fast_curator
        import fast_flow.v1
        from . import backends, bookkeeping, data_import
        from .settings import CarpenterSettings
    except ImportError as e:
        rich.print("[red]Failed to import required package:[/red]", e)
        sys.exit(1)

    sequence, sequence_cfg = fast_flow.v1.read_sequence_yaml(
        sequence_cfg,
        output_dir=output_dir,
        backend="fast_carpenter",
        return_cfg=True,
    )
    datasets = fast_curator.read.from_yaml(dataset_cfg)
    backend = backends.get_backend(processing_backend)

    mkdir_p(output_dir)

    if store_bookkeeping:
        book_keeping_file = os.path.join(output_dir, "book-keeping.tar.gz")
        bookkeeping.write_booking(
            book_keeping_file, sequence_cfg, datasets, cmd_line_args=sys.argv[1:]
        )
        # fast_carpenter.store_bookkeeping(datasets, output_dir)
    settings = CarpenterSettings(
        ncores=1,
        outdir=output_dir,
    )
    results, _ = backend.execute(
        sequence,
        datasets,
        args=settings,
        plugins={"data_import": data_import.get_data_import_plugin("uproot4", None)},
    )
    rich.print(f"[blue]Results[/]: {results}")
    rich.print(f"[blue]Output written to directory {output_dir}[/]")


if __name__ == "__main__":
    typer.run(main)
