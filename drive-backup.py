#!/usr/bin/env python3
import os
import sys
import subprocess
import datetime
import json
import logging
from logging.handlers import RotatingFileHandler
import concurrent.futures

# --- Configuration ---
RCLONE_REMOTE = "gdrive"
BASE_REMOTE_PATH = "backups/automated_server_backups"
MAX_BACKUPS = 14

TIMESTAMP = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_FILE_PATH = "/root/drive-backup-logs.txt"
CURRENT_BACKUP_REMOTE_DIR = f"{BASE_REMOTE_PATH}/backup_{TIMESTAMP}"

DB_USER = "prestashop"
DB_PASS = "REPLACE_ME"
DB_NAME = "prestashop"

BACKUP_TARGETS = {
    "apache2_config": "/etc/apache2",
    "letsencrypt_config": "/etc/letsencrypt",
    "wireguard_data": "/var/lib/docker/volumes/wg-easy_etc_wireguard/_data/",
    "wireguard_compose": "/root/wg-easy",
    "website_html": "/var/www/html",
    "terraria_server": "/home/amp/.ampdata/instances/Terraria_Vanilla_02_202601/Terraria",
    "minecraft_server": "/home/amp/.ampdata/instances/Cobblemon_1_21_101/Minecraft",
    "ufw_etc": "/etc/ufw",
    "ufw_defaults": "/etc/default/ufw",
    "drive_backup_script": "/root/drive-backup.py",
    "php_config": "/etc/php/"
}

def setup_logger():
    """Configures a rotating logger ensuring strict 0o700 file permissions."""
    if not os.path.exists(LOG_FILE_PATH):
        # Create file with restrictive permissions immediately
        fd = os.open(LOG_FILE_PATH, os.O_CREAT | os.O_WRONLY, 0o700)
        os.close(fd)
    else:
        # Enforce permissions if file already exists
        os.chmod(LOG_FILE_PATH, 0o700)

    logger = logging.getLogger("BackupLogger")
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        # 10MB rotation, keep 1 backup file (.1)
        handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=10*1024*1024, backupCount=1)
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        
        logger.addHandler(handler)
        logger.addHandler(console)
        
    return logger

log = setup_logger()

def run_rclone_cmd(args, check=True):
    cmd = ["rclone"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log.error(f"Rclone command failed: {' '.join(cmd)}")
        log.error(f"Rclone Stderr: {result.stderr}")
        if check:
            raise Exception(f"Rclone failed: {result.stderr}")
    return result.stdout

def backup_database_stream():
    """Streams DB -> Gzip -> Rclone (No local disk usage)."""
    filename = "database.sql.gz"
    remote_dest = f"{RCLONE_REMOTE}:{CURRENT_BACKUP_REMOTE_DIR}/{filename}"
    log.info(f"Starting Database Stream to: {remote_dest}")

    env = os.environ.copy()
    env['MYSQL_PWD'] = DB_PASS

    # --- PIPELINE CONSTRUCTION ---
    # Data flows from p1_dump -> p2_gzip -> p3_rclone in memory.
    
    # 1. Start Mysqldump (Producer)
    p1_dump = subprocess.Popen(
        ["mysqldump", "-u", DB_USER, DB_NAME, "--single-transaction", "--quick"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env
    )

    # 2. Start Gzip (Filter) - reads from p1's stdout
    p2_gzip = subprocess.Popen(
        ["gzip", "-c"],
        stdin=p1_dump.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # 3. Start Rclone (Consumer) - reads from p2's stdout
    p3_rclone = subprocess.Popen(
        ["rclone", "rcat", remote_dest, "--stats", "5s"],
        stdin=p2_gzip.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # CRITICAL: Close parent copies of the file descriptors.
    # If we don't do this, the child processes (gzip/rclone) will never receive 
    # an EOF (End Of File) signal because the parent process still holds the pipe open, 
    # causing the pipeline to hang indefinitely.
    p1_dump.stdout.close()
    p2_gzip.stdout.close()

    # Wait for the end of the pipeline and capture stderr
    rclone_out, rclone_err = p3_rclone.communicate()
    gzip_out, gzip_err = p2_gzip.communicate()
    dump_out, dump_err = p1_dump.communicate()

    if p3_rclone.returncode != 0:
        raise Exception(f"Rclone DB Upload Failed: {rclone_err.decode()}")

    if p1_dump.returncode != 0:
        raise Exception(f"Mysqldump Failed: {dump_err.decode()}")

    log.info("Database stream completed successfully.")

def backup_files_stream():
    """Streams Tar -> Rclone (No local disk usage) with full error handling."""
    filename = "files.tar.gz"
    remote_dest = f"{RCLONE_REMOTE}:{CURRENT_BACKUP_REMOTE_DIR}/{filename}"
    log.info(f"Starting File Stream to: {remote_dest}")

    tar_cmd = ["tar", "-czf", "-", "-C", "/"]

    valid_targets_count = 0
    for name, path in BACKUP_TARGETS.items():
        if os.path.exists(path):
            tar_cmd.append(path.lstrip("/"))
            valid_targets_count += 1
        else:
            log.warning(f"Not Found: {path}")

    if valid_targets_count == 0:
        log.error("No valid targets found to backup! Aborting file stream.")
        return

    # 1. Start Tar (Producer)
    p1_tar = subprocess.Popen(
        tar_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # 2. Start Rclone (Consumer)
    p2_rclone = subprocess.Popen(
        ["rclone", "rcat", remote_dest, "--stats", "10s"],
        stdin=p1_tar.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Close parent handle to allow pipe to trigger EOF on completion
    p1_tar.stdout.close()

    rclone_out, rclone_err = p2_rclone.communicate()
    tar_out, tar_err = p1_tar.communicate()

    if p2_rclone.returncode != 0:
        log.error(f"Rclone File Upload Failed: {rclone_err.decode()}")
        raise Exception("File stream upload failed")

    if p1_tar.returncode > 1:
        log.error(f"Tar Fatal Error: {tar_err.decode()}")
        raise Exception("Tar compression failed")
    elif p1_tar.returncode == 1:
        log.warning("Tar warning (files changed during read), continuing...")

    log.info("File stream completed successfully.")

def manage_retention():
    """Groups backups by folder and deletes old ones."""
    log.info("Checking retention policy...")

    ls_out = run_rclone_cmd(["lsjson", f"{RCLONE_REMOTE}:{BASE_REMOTE_PATH}", "--dirs-only"])
    dirs = json.loads(ls_out)

    backup_dirs = [d for d in dirs if d['Name'].startswith('backup_')]
    backup_dirs.sort(key=lambda x: x['Name'])

    log.info(f"Found {len(backup_dirs)} backup folders.")

    if len(backup_dirs) > MAX_BACKUPS:
        to_delete = backup_dirs[:len(backup_dirs) - MAX_BACKUPS]
        log.info(f"Retention exceeded. Deleting {len(to_delete)} old folders.")

        for item in to_delete:
            folder_name = item['Name']
            log.info(f"Purging old backup folder: {folder_name}")
            run_rclone_cmd(["purge", f"{RCLONE_REMOTE}:{BASE_REMOTE_PATH}/{folder_name}"])
    else:
        log.info("Retention limit not reached.")

def perform_backup():
    log.info(f"Backup job started. ID: {TIMESTAMP}")

    try:
        # 1. Create Remote Directory
        run_rclone_cmd(["mkdir", f"{RCLONE_REMOTE}:{CURRENT_BACKUP_REMOTE_DIR}"], check=False)

        # 2. Run Streams in Parallel
        # ThreadPoolExecutor is ideal here since subprocesses release the GIL and are I/O bound
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_db = executor.submit(backup_database_stream)
            future_files = executor.submit(backup_files_stream)

            # Wait for both threads and raise any exceptions caught during execution
            for future in concurrent.futures.as_completed([future_db, future_files]):
                future.result()

        # 3. Retention
        manage_retention()

    except Exception as e:
        log.error(f"Backup failed: {e}")

    finally:
        log.info("Backup finished.")

if __name__ == "__main__":
    if os.geteuid() != 0:
        print("Error: Must run as root.")
        sys.exit(1)
    perform_backup()
