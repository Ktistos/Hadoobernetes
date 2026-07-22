"""
Main entry point for the Hadoobernetes CLI.

Uses the Click library to parse user arguments, orchestrate data uploads, 
and interact with the Cluster Manager.
"""

import click
import os
from pprint import pprint

import auth
import storage
import api_client

class HadoobCLI(click.Group):
    """Custom Click Group to organize the help menu into sections."""
    def format_commands(self, ctx, formatter):
        # Fetch all registered commands
        commands = {cmd_name: self.get_command(ctx, cmd_name) for cmd_name in self.list_commands(ctx)}
        
        # Define the categories and the exact order they should appear
        groups = {
            "Authentication": ["login", "logout"],
            "Data & Storage": ["upload", "download"],
            "Map-Reduce Operations": ["submit", "status", "abort"]
        }

        # Print each section nicely formatted
        for group_name, cmd_names in groups.items():
            with formatter.section(group_name):
                formatter.write_dl(
                    [(name, commands[name].get_short_help_str(60)) for name in cmd_names if name in commands]
                )

# Apply our custom class to the main CLI group
@click.group(cls=HadoobCLI)
def cli():
    """Hadoobernetes: The Python Kubernetes Map-Reduce CLI."""
    pass

@cli.command()
@click.option('--username', prompt=True, help='Your Keycloak username.')
@click.option('--password', prompt=True, hide_input=True, help='Your Keycloak password.')
def login(username, password):
    """Authenticate with the Hadoobernetes cluster."""
    try:
        auth.login(username, password)
        click.secho(f"Successfully logged in as {username}.", fg="green")
    except Exception as e:
        click.secho(str(e), fg="red")

@cli.command()
def logout():
    """Log out and clear local credentials."""
    from auth import TOKEN_FILE
    import os

    if TOKEN_FILE.exists():
        os.remove(TOKEN_FILE)
        click.echo("Successfully logged out.")
    else:
        click.echo("You are not currently logged in.")

@cli.command()
@click.option('--mappers', type=int, required=True, help='Number of map tasks to spawn.')
@click.option('--reducers', type=int, required=True, help='Number of reduce tasks to spawn.')
@click.option('--input-file', type=click.Path(exists=True), required=True, help='Local path to the raw data file.')
@click.option('--code', type=click.Path(exists=True), required=True, help='Local path to the Python executable script.')
def submit(mappers, reducers, input_file, code):
    """Stage local files and submit a new Map-Reduce job."""
    try:
        user_id = auth.get_current_user_id()
        staging_prefix = f"users/{user_id}/staged_inputs"
        
        # click.echo("[*] Uploading input data to MinIO...")
        # input_data_path = storage.upload_file(input_file, f"{staging_prefix}/data")
        # click.echo(f"  -> {input_data_path}")
        
        # click.echo("[*] Uploading executable code to MinIO...")
        # code_location = storage.upload_file(code, f"{staging_prefix}/code")
        # click.echo(f"  -> {code_location}")
        
        # # Calculate file size for chunking
        # input_file_size_bytes = os.path.getsize(input_file)
        
        # # Define where the system should put the final merged results
        # output_data_path = f"minio://mapreduce/{staging_prefix}/outputs/"
        
        # # Build the payload
        # payload = {
        #     "num_mappers": mappers,
        #     "num_reducers": reducers,
        #     "input_data_path": input_data_path,
        #     "output_data_path": output_data_path,
        #     "code_location": code_location,
        #     "input_file_size_bytes": input_file_size_bytes
        # }
        
        click.echo("[*] Uploading input data to MinIO...")
        raw_input_data_path = storage.upload_file(input_file, f"{staging_prefix}/data")
        click.echo(f"  -> {raw_input_data_path}")
        
        click.echo("[*] Uploading executable code to MinIO...")
        raw_code_location = storage.upload_file(code, f"{staging_prefix}/code")
        click.echo(f"  -> {raw_code_location}")
        
        input_file_size_bytes = os.path.getsize(input_file)
        
        input_data_path = raw_input_data_path.replace(f"minio://{storage.BUCKET}/", "")
        code_location = raw_code_location.replace(f"minio://{storage.BUCKET}/", "")
        output_data_path = f"{staging_prefix}/outputs/"
        
        payload = {
            "num_mappers": mappers,
            "num_reducers": reducers,
            "input_data_path": input_data_path,
            "output_data_path": output_data_path,
            "code_location": code_location,
            "input_file_size_bytes": input_file_size_bytes
        }

        click.echo("[*] Submitting job to Cluster Manager...")
        response = api_client.submit_job(payload)
        
        click.secho(f"\nSuccess! Job ID: {response['job_id']}", fg="green")
        click.echo(f"To check status, run: hadoob status {response['job_id']}")
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg="red")

@cli.command()
@click.argument('job_id')
def status(job_id):
    """Fetch the real-time execution status of a job."""
    try:
        data = api_client.get_status(job_id)
        click.secho(f"Status for Job {job_id}:", fg="blue", bold=True)
        pprint(data)
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg="red")

@cli.command()
@click.argument('job_id')
def abort(job_id):
    """Forcefully terminate an active job."""
    if click.confirm(f"Are you sure you want to abort job {job_id}?"):
        try:
            response = api_client.abort_job(job_id)
            click.secho(response.get("message", "Job aborted."), fg="green")
        except Exception as e:
            click.secho(f"Error: {str(e)}", fg="red")

@cli.command()
@click.argument('local_path', type=click.Path(exists=True))
@click.argument('remote_path')
def upload(local_path, remote_path):
    """Upload a local file to your MinIO bucket."""
    from storage import upload_file
    from auth import get_current_user_id
    
    try:
        user_id = get_current_user_id()
        # Prefix the remote path with the user's isolated directory
        destination_prefix = f"users/{user_id}/{remote_path}"
        url = upload_file(local_path, destination_prefix)
        click.echo(f"Uploaded: {url}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument('remote_path')
@click.argument('local_path', type=click.Path())
def download(remote_path, local_path):
    """Download a file from your MinIO bucket to your local machine."""
    from auth import get_current_user_id
    from storage import download_file
    try:
        user_id = get_current_user_id()
        full_remote_path = f"users/{user_id}/{remote_path}"
        download_file(full_remote_path, local_path)
        click.echo(f"Successfully downloaded {full_remote_path} to {local_path}")
    except Exception as e:
        click.echo(f"Download failed: {e}")
        
if __name__ == '__main__':
    cli()