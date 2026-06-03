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
        commands = {cmd_name: self.get_command(ctx, cmd_name) for cmd_name in self.list_commands(ctx)}
        groups = {
            "Authentication": ["login", "logout"],
            "Data & Storage": ["upload", "download", "get-output"],
            "Map-Reduce Operations": ["submit", "status", "abort"]
        }
        for group_name, cmd_names in groups.items():
            with formatter.section(group_name):
                formatter.write_dl(
                    [(name, commands[name].get_short_help_str(60)) for name in cmd_names if name in commands]
                )
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
        import uuid
        from auth import get_current_user_id
        user_id = get_current_user_id()
        # Per-user, per-submission staging prefix so concurrent submissions
        # never overwrite each other's input or code.
        submission_id = uuid.uuid4().hex[:12]
        staging_prefix = f"users/{user_id}/staged/{submission_id}"
        click.echo("[*] Uploading input data to MinIO...")
        raw_input_data_path = storage.upload_file(input_file, f"{staging_prefix}/data")
        click.echo(f"  -> {raw_input_data_path}")
        click.echo("[*] Uploading executable code to MinIO...")
        raw_code_location = storage.upload_file(code, f"{staging_prefix}/code")
        click.echo(f"  -> {raw_code_location}")
        input_file_size_bytes = os.path.getsize(input_file)
        input_data_path = raw_input_data_path.replace(f"minio://{storage.BUCKET}/", "")
        code_location = raw_code_location.replace(f"minio://{storage.BUCKET}/", "")
        payload = {
            "num_mappers": mappers,
            "num_reducers": reducers,
            "input_data_path": input_data_path,
            "code_location": code_location,
            "input_file_size_bytes": input_file_size_bytes
        }
        click.echo("[*] Submitting job to Cluster Manager...")
        response = api_client.submit_job(payload)
        click.secho(f"\nSuccess! Job ID: {response['job_id']}", fg="green")
        click.echo(f"To check status, run: hadoob status {response['job_id']}")
        click.echo(f"To fetch results when complete, run: hadoob get-output {response['job_id']} ./results")
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
@cli.command(name="get-output")
@click.argument('job_id')
@click.argument('local_dir', type=click.Path(), default="./results")
def get_output(job_id, local_dir):
    """Download all output part files for a completed job into LOCAL_DIR."""
    from auth import get_current_user_id
    from storage import download_prefix
    try:
        # Best-effort status check so we give a useful message instead of an
        # empty download if the job hasn't finished writing output yet.
        try:
            data = api_client.get_status(job_id)
            job_status = data.get("status")
            if job_status and job_status != "completed":
                click.secho(
                    f"Job {job_id} is '{job_status}', not 'completed'. "
                    f"Output may be missing or partial.",
                    fg="yellow",
                )
        except Exception:
            click.secho("Could not verify job status; attempting download anyway.", fg="yellow")
        user_id = get_current_user_id()
        output_prefix = f"users/{user_id}/jobs/{job_id}/output/"
        click.echo(f"[*] Fetching output from {output_prefix} ...")
        written = download_prefix(output_prefix, local_dir)
        if not written:
            click.secho(
                f"No output files found under {output_prefix}. "
                f"Has the job completed?",
                fg="red",
            )
            return
        click.secho(f"Downloaded {len(written)} file(s) to {local_dir}:", fg="green")
        for path in written:
            click.echo(f"  -> {path}")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red")
if __name__ == '__main__':
    cli()