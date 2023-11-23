import click

@click.group()
def cli():
    "Chord implementation for wikipedia's computer scientists dataset."
    pass



@cli.command()
@click.argument('university', type = str)
@click.argument('awards', type = int)
def lookup(university: str, awards):
    """
    Distributed lookup for computer scientists having a minimum number of awards in the chord.
    """
    click.echo(f"Searching for {university} with {awards} awards.")

@cli.command()
@click.argument('university', type = str)
@click.argument('awards', type = int)
def join(university: str, awards):
    """
    Distributed lookup for computer scientists having a minimum number of awards in the chord.
    """
    click.echo(f"Joining for {university} with {awards} awards.")

@cli.command()
@click.argument('university', type = str)
@click.argument('awards', type = int)
def leave(university: str, awards):
    """
    Distributed lookup for computer scientists having a minimum number of awards in the chord.
    """
    click.echo(f"Leaving for {university} with {awards} awards.")




if __name__ == '__main__':
    cli()