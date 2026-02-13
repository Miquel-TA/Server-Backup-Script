#!/usr/bin/env python3
import os
import sys
import subprocess
import datetime
import shutil
import json

# --- Configuration ---
RCLONE_REMOTE = "gdrive"
BASE_REMOTE_PATH = "backups/automated_server_backups"
MAX_BACKUPS = 14

TIMESTAMP = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
TEMP_DIR = "/root/drive-backup-temp-path"
LOG_FILE_PATH = os.path.join(TEMP_DIR, f"backup_log_{TIMESTAMP}.txt")

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

def ensure_temp_dir():
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR, mode=0o700, exist_ok=True)

def log(message, level="INFO"):
    ensure_temp_dir()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] [{level}] {message}"
    print(entry)
    with open(LOG_FILE_PATH, "a") as f:
        f.write(entry + "\n")

def run_rclone_cmd(args, check=True):
    cmd = ["rclone"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log(f"Rclone command failed: {' '.join(cmd)}", "ERROR")
        log(f"Rclone Stderr: {result.stderr}", "ERROR")
        if check:
            raise Exception(f"Rclone failed: {result.stderr}")
    return result.stdout

def backup_database_stream():
    """Streams DB -> Gzip -> Rclone (No local disk usage)."""
    filename = "database.sql.gz"
    remote_dest = f"{RCLONE_REMOTE}:{CURRENT_BACKUP_REMOTE_DIR}/{filename}"
    log(f"Starting Database Stream to: {remote_dest}")

    env = os.environ.copy()
    env['MYSQL_PWD'] = DB_PASS

    # 1. Start Mysqldump (Producer)
    p1_dump = subprocess.Popen(
        ["mysqldump", "-u", DB_USER, DB_NAME, "--single-transaction", "--quick"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env
    )

    # 2. Start Gzip (Filter)
    p2_gzip = subprocess.Popen(
        ["gzip", "-c"],
        stdin=p1_dump.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # 3. Start Rclone (Consumer)
    p3_rclone = subprocess.Popen(
        ["rclone", "rcat", remote_dest, "--stats", "5s"],
        stdin=p2_gzip.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Close parent handles to allow pipes to flow and close naturally
    p1_dump.stdout.close()
    p2_gzip.stdout.close()

    # Wait for completion and capture outputs
    rclone_out, rclone_err = p3_rclone.communicate()
    gzip_out, gzip_err = p2_gzip.communicate()
    dump_out, dump_err = p1_dump.communicate()

    # Check Exit Codes
    if p3_rclone.returncode != 0:
        raise Exception(f"Rclone DB Upload Failed: {rclone_err.decode()}")

    if p1_dump.returncode != 0:
        raise Exception(f"Mysqldump Failed: {dump_err.decode()}")

    log("Database stream completed successfully.")

def backup_files_stream():
    """Streams Tar -> Rclone (No local disk usage) with full error handling."""
    filename = "files.tar.gz"
    remote_dest = f"{RCLONE_REMOTE}:{CURRENT_BACKUP_REMOTE_DIR}/{filename}"
    log(f"Starting File Stream to: {remote_dest}")

    # Build Tar Command
    # -c: create, -z: gzip, -f -: output to stdout, -P: start from root dir
    tar_cmd = ["tar", "-czf", "-", "-C", "/"]

    valid_targets_count = 0
    for name, path in BACKUP_TARGETS.items():
        if os.path.exists(path):
            tar_cmd.append(path.lstrip("/"))
            valid_targets_count += 1
            #log(f"  [+] Added: {path}")
        else:
            log(f"Not Found: {path}", "WARN")

    if valid_targets_count == 0:
        log("No valid targets found to backup! Aborting file stream.", "ERROR")
        return

    # Pipeline Execution: Tar -> Rclone

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

    # Close parent handle to allow pipe to close
    p1_tar.stdout.close()

    # Wait for completion
    rclone_out, rclone_err = p2_rclone.communicate()
    tar_out, tar_err = p1_tar.communicate()

    # Check Rclone Exit Code (Strict)
    if p2_rclone.returncode != 0:
        log(f"Rclone File Upload Failed: {rclone_err.decode()}", "ERROR")
        raise Exception("File stream upload failed")

    # Check Tar Exit Code (Lenient)
    # 0 = Success
    # 1 = Files changed while reading (Warning) -> ACCEPTABLE
    # >1 = Fatal Error -> RAISE
    if p1_tar.returncode > 1:
        log(f"Tar Fatal Error: {tar_err.decode()}", "ERROR")
        raise Exception("Tar compression failed")
    elif p1_tar.returncode == 1:
        log("Tar warning (files changed during read), continuing...", "WARN")

    log("File stream completed successfully.")

def manage_retention():
    """Groups backups by folder and deletes old ones."""
    log("Checking retention policy...")

    # List directories only in the base path
    ls_out = run_rclone_cmd(["lsjson", f"{RCLONE_REMOTE}:{BASE_REMOTE_PATH}", "--dirs-only"])
    dirs = json.loads(ls_out)

    # Filter for backup folders (e.g., backup_2025...)
    backup_dirs = [d for d in dirs if d['Name'].startswith('backup_')]
    backup_dirs.sort(key=lambda x: x['Name'])

    log(f"Found {len(backup_dirs)} backup folders.")

    if len(backup_dirs) > MAX_BACKUPS:
        to_delete = backup_dirs[:len(backup_dirs) - MAX_BACKUPS]
        log(f"Retention exceeded. Deleting {len(to_delete)} old folders.")

        for item in to_delete:
            folder_name = item['Name']
            log(f"Purging old backup folder: {folder_name}")
            run_rclone_cmd(["purge", f"{RCLONE_REMOTE}:{BASE_REMOTE_PATH}/{folder_name}"])
    else:
        log("Retention limit not reached.")

def perform_backup():
    ensure_temp_dir()
    log(f"Backup job started. ID: {TIMESTAMP}")

    try:
        # 1. Create Remote Directory
        run_rclone_cmd(["mkdir", f"{RCLONE_REMOTE}:{CURRENT_BACKUP_REMOTE_DIR}"], check=False)

        # 2. Streams
        backup_database_stream()
        backup_files_stream()

        # 3. Retention
        manage_retention()

    except Exception as e:
        log(f"Backup failed: {e}", "ERROR")

    finally:
        log("Backup finished. Uploading logs and cleaning up.")
        # Upload Log to the same folder
        subprocess.run(
            ["rclone", "copy", LOG_FILE_PATH, f"{RCLONE_REMOTE}:{CURRENT_BACKUP_REMOTE_DIR}"],
            check=False
        )
        # Clean local temp
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)

if __name__ == "__main__":
    if os.geteuid() != 0:
        print("Error: Must run as root.")
        sys.exit(1)
    perform_backup()
