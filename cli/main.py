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

@click.group()
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
@click.option('--mappers', type=int, required=True, help='Number of map tasks to spawn.')
@click.option('--reducers', type=int, required=True, help='Number of reduce tasks to spawn.')
@click.option('--input-file', type=click.Path(exists=True), required=True, help='Local path to the raw data file.')
@click.option('--code', type=click.Path(exists=True), required=True, help='Local path to the Python executable script.')
def submit(mappers, reducers, input_file, code):
    """Stage local files and submit a new Map-Reduce job."""
    try:
        # 1. We need a unique prefix for this user's uploads. 
        # For a production CLI, you might extract the user ID from the JWT. 
        # Here we'll use a generic staging prefix for simplicity.
        staging_prefix = "users/staged_inputs"
        
        click.echo("[*] Uploading input data to MinIO...")
        input_data_path = storage.upload_file(input_file, f"{staging_prefix}/data")
        click.echo(f"  -> {input_data_path}")
        
        click.echo("[*] Uploading executable code to MinIO...")
        code_location = storage.upload_file(code, f"{staging_prefix}/code")
        click.echo(f"  -> {code_location}")
        
        # Calculate file size for chunking
        input_file_size_bytes = os.path.getsize(input_file)
        
        # Define where the system should put the final merged results
        output_data_path = f"minio://mapreduce/{staging_prefix}/outputs/"
        
        # Build the payload
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

if __name__ == '__main__':
    cli()