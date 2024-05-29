import click

from typorio import core


@click.group()
def cli():
    pass


cli.add_command(core.start, name="start")  # noqa


def run_command(args):
    from click.testing import CliRunner

    runner = CliRunner()
    runner.invoke(cli, args)


if __name__ == "__main__":
    run_command(["start"])
