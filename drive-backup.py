import os
import sys
import subprocess
import datetime
import json
import logging
import concurrent.futures
import io
import smtplib
import base64
import urllib.request
import urllib.parse
import threading
import time
from email.message import EmailMessage

# Sensitive config setup
SENSITIVE_CONFIG_FILE = "/root/.config/drive-backup/variables.conf"
def load_config(filepath):
    """Loads configuration variables from a KEY=VALUE file."""
    config = {}
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    return config

config = load_config(SENSITIVE_CONFIG_FILE)

# Validate essential configuration keys to prevent runtime KeyError
REQUIRED_KEYS = [
    "DRIVE_BASE_FOLDER_ID", "DB_USER", "DB_PASS", "DB_NAME", 
    "EMAIL_TO", "EMAIL_FROM", "OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET", 
    "OAUTH_REFRESH_TOKEN_GMAIL", "OAUTH_REFRESH_TOKEN_DRIVE"
]
for key in REQUIRED_KEYS:
    if key not in config:
        print(f"Error: Missing {key} in configuration file.")
        sys.exit(1)

# Backup settings
BASE_FOLDER_ID = config["DRIVE_BASE_FOLDER_ID"]
MAX_BACKUPS = 14

# Starting timestamp for backups and emails
TIMESTAMP = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
CURRENT_BACKUP_NAME = f"backup_{TIMESTAMP}"

# Sensitive configs
DB_USER = config["DB_USER"]
DB_PASS = config["DB_PASS"]
DB_NAME = config["DB_NAME"]

EMAIL_TO = config["EMAIL_TO"]
EMAIL_FROM = config["EMAIL_FROM"]

OAUTH_CLIENT_ID = config["OAUTH_CLIENT_ID"]
OAUTH_CLIENT_SECRET = config["OAUTH_CLIENT_SECRET"]
OAUTH_REFRESH_TOKEN_GMAIL = config["OAUTH_REFRESH_TOKEN_GMAIL"]
OAUTH_REFRESH_TOKEN_DRIVE = config["OAUTH_REFRESH_TOKEN_DRIVE"]

# Sources to backup
BACKUP_TARGETS = {
    "apache2_config": "/etc/apache2",
    "letsencrypt_config": "/etc/letsencrypt",
    "wireguard_data": "/var/lib/docker/volumes/wg-easy_etc_wireguard/_data/",
    "wireguard_compose": "/root/wg-easy",
    "website_html": "/var/www/html",
    "terraria_infernum_server": "/home/amp/.ampdata/instances/Infernum01/tModLoader/serverfiles/",
#   "terraria_vanilla_server": "/home/amp/.ampdata/instances/Terraria_Vanilla_02_202601/Terraria",
    "cobblemon_server": "/home/amp/.ampdata/instances/Cobblemon_1_21_101/Minecraft",
#    "semivanilla_server_paula": "/home/amp/.ampdata/instances/Modded_PolPaula_121101/Minecraft",
#    "semivanilla_server_iyo": "/home/amp/.ampdata/instances/Modded_Iyo_121101/Minecraft",
    "ufw_etc": "/etc/ufw",
    "ufw_defaults": "/etc/default/ufw",
    "drive_backup_script": "/root/drive-backup.py",
    "drive_backup_downloader": "/root/drive-download.py",
    "php_config": "/etc/php/"
}

# Global state variables
DRIVE_ACCESS_TOKEN = None
GMAIL_ACCESS_TOKEN = None
TARGET_FOLDER_ID = None

# Logging system
log_stream = io.StringIO()
def setup_logger():
    logger = logging.getLogger("BackupLogger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        memory_handler = logging.StreamHandler(log_stream)
        memory_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(memory_handler)
        logger.addHandler(console_handler)
    return logger
log = setup_logger()

# Get token for Gmail or Drive
def get_access_token(refresh_token):
    url = "https://oauth2.googleapis.com/token"
    params = {
        "client_id": OAUTH_CLIENT_ID,
        "client_secret": OAUTH_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
    data = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(url, data=data)

    with urllib.request.urlopen(req, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))
        return result["access_token"]

# Stops all AMP instances
def stop_amp_instances():
    log.info("Stopping all AMP instances...")
    res = subprocess.run(["sudo", "-u", "amp", "ampinstmgr", "--StopAllInstances"], capture_output=True, text=True)
    if res.returncode != 0:
        log.error(f"Failed to stop AMP instances: {res.stderr.strip()}")
        raise Exception(f"AMP StopAllInstances failed: {res.stderr.strip()}")
    log.info("All AMP instances stopped successfully.")

# Upgrades AMP instances
def upgrade_amp_instances():
    log.info("Upgrading AMP instances...")
    res_up = subprocess.run(["sudo", "-u", "amp", "ampinstmgr", "--UpgradeAll"], capture_output=True, text=True)
    if res_up.returncode != 0:
        log.error(f"Failed to upgrade AMP instances: {res_up.stderr.strip()}")
    else:
        log.info("AMP instances upgraded successfully.")

    log.info("Starting ADS01 controller instance...")
    res_ads = subprocess.run(["sudo", "-u", "amp", "ampinstmgr", "--StartInstance", "ADS01"], capture_output=True, text=True)
    if res_ads.returncode != 0:
        log.error(f"Failed to start ADS01 instance: {res_ads.stderr.strip()}")
    else:
        log.info("ADS01 instance started successfully.")

# Send email via smtp using oauth2
def send_email_report(status):
    log.info("Preparing email report...")

    msg = EmailMessage()
    msg['Subject'] = f"Backup Report [{status}] started at [{TIMESTAMP}]"
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO

    log_contents = log_stream.getvalue()
    msg.set_content(f"Backup Status: {status}\n\nBackup Logs:\n{'-'*40}\n{log_contents}")

    try:
        gmail_token = get_access_token(OAUTH_REFRESH_TOKEN_GMAIL)
        auth_string = f"user={EMAIL_FROM}\x01auth=Bearer {gmail_token}\x01\x01"
        auth_encoded = base64.b64encode(auth_string.encode('ascii')).decode('ascii')

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()

        code, resp = server.docmd("AUTH", f"XOAUTH2 {auth_encoded}")
        if code != 235:
            error_details = base64.b64decode(resp).decode('utf-8') if resp else "No details"
            raise Exception(f"SMTP Auth Rejected (Code {code}): {error_details}")

        server.send_message(msg)
        server.quit()
        log.info("Email report sent successfully.")
    except Exception as e:
        log.error(f"Error sending email: {e}")

# Creates a folder in Google Drive and returns its ID
def create_drive_folder(name, parent_id):
    drive_token = get_access_token(OAUTH_REFRESH_TOKEN_DRIVE)
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    req = urllib.request.Request(
        "https://www.googleapis.com/drive/v3/files",
        data=json.dumps(metadata).encode('utf-8'),
        headers={
            "Authorization": f"Bearer {drive_token}",
            "Content-Type": "application/json"
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))['id']

# Gets a resumable upload URL from Google Drive API
def get_resumable_upload_url(filename):
    drive_token = get_access_token(OAUTH_REFRESH_TOKEN_DRIVE)
    metadata = {"name": filename, "parents": [TARGET_FOLDER_ID]}
    req = urllib.request.Request(
        "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable",
        data=json.dumps(metadata).encode('utf-8'),
        headers={
            "Authorization": f"Bearer {drive_token}",
            "Content-Type": "application/json; charset=UTF-8"
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.getheader('Location')

# Dumps DB, gzips it and uploads it to gdrive using purely in-memory managed pipes
def backup_database_stream():
    filename = "database.sql.gz"
    log.info(f" +Streaming DB {DB_NAME} to {filename}")

    env = os.environ.copy()
    env['MYSQL_PWD'] = DB_PASS
    upload_url = get_resumable_upload_url(filename)

    with subprocess.Popen(
        ["mysqldump", "-u", DB_USER, DB_NAME, "--single-transaction", "--quick"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    ) as p1_dump:

        dump_err_mem = []
        err_thread = threading.Thread(target=lambda: dump_err_mem.append(p1_dump.stderr.read()), daemon=True)
        err_thread.start()

        with subprocess.Popen(
            ["gzip", "-c"],
            stdin=p1_dump.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        ) as p2_gzip:

            with subprocess.Popen(
                ["curl", "-sS", "--fail", "-X", "PUT", "-T", "-", upload_url],
                stdin=p2_gzip.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            ) as p3_curl:

                # Allow earlier processes to receive SIGPIPE if curl drops connection
                p1_dump.stdout.close()
                p2_gzip.stdout.close()

                curl_out, curl_err = p3_curl.communicate()
                p2_gzip.wait()
                p1_dump.wait()
                err_thread.join()

                if p3_curl.returncode != 0:
                    raise Exception(f"Drive DB cURL Upload Failed (Code {p3_curl.returncode}): {curl_err.decode('utf-8', errors='replace').strip()}")

                if p1_dump.returncode != 0:
                    err_details = (dump_err_mem[0] if dump_err_mem else b'').decode('utf-8', errors='replace').strip()
                    raise Exception(f"Mysqldump Failed: {err_details or f'Exit code {p1_dump.returncode}'}")

    log.info("Database stream completed successfully.")

# Compresses the files and directories and uploads them to gdrive using purely in-memory managed pipes
def backup_files_stream():
    filename = "files.tar.gz"
    tar_cmd = ["tar", "-czf", "-", "--warning=no-file-changed", "-C", "/"]

    valid_targets_count = 0
    for name, path in BACKUP_TARGETS.items():
        if os.path.exists(path):
            log.info(f" +Streaming {name} to {filename}")
            tar_cmd.append(path.lstrip("/"))
            valid_targets_count += 1
        else:
            log.warning(f"Not Found: {path}")

    if valid_targets_count == 0:
        log.error("No valid targets found to backup! Aborting file stream.")
        return

    upload_url = get_resumable_upload_url(filename)

    with subprocess.Popen(
        tar_cmd,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as p1_tar:

        tar_err_mem = []
        err_thread = threading.Thread(target=lambda: tar_err_mem.append(p1_tar.stderr.read()), daemon=True)
        err_thread.start()

        with subprocess.Popen(
            ["curl", "-sS", "--fail", "-X", "PUT", "-T", "-", upload_url],
            stdin=p1_tar.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as p2_curl:

            p1_tar.stdout.close()

            curl_out, curl_err = p2_curl.communicate()
            p1_tar.wait()
            err_thread.join()

            if p2_curl.returncode != 0:
                err_details = curl_err.decode('utf-8', errors='replace').strip() or "Unknown cURL error"
                raise Exception(f"File stream upload failed (Code {p2_curl.returncode}): {err_details}")

            if p1_tar.returncode != 0:
                err_details = (tar_err_mem[0] if tar_err_mem else b'').decode('utf-8', errors='replace').strip()
                raise Exception(f"Tar compression failed: {err_details or f'Exit code {p1_tar.returncode}'}")

    log.info("File stream completed successfully.")

# Wrapper to retry upload tasks
def retry_upload(func, max_retries=3, wait_seconds=60):
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                log.error(f"Task '{func.__name__}' failed on attempt {attempt}/{max_retries}. No more retries. Error: {e}")
                raise
            log.warning(f"Task '{func.__name__}' failed on attempt {attempt}/{max_retries}. Retrying in {wait_seconds} seconds... Error: {e}")
            time.sleep(wait_seconds)

# Deletes old backups
def manage_retention():
    log.info("Checking retention policy...")
    drive_token = get_access_token(OAUTH_REFRESH_TOKEN_DRIVE)

    query = f"'{BASE_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and name contains 'backup_' and trashed=false"
    url = f"https://www.googleapis.com/drive/v3/files?q={urllib.parse.quote(query)}&fields=files(id,name)&orderBy=name"

    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {drive_token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        dirs = json.loads(resp.read().decode('utf-8')).get('files', [])

    log.info(f"Found {len(dirs)} backup folders.")

    if len(dirs) > MAX_BACKUPS:
        to_delete = dirs[:len(dirs) - MAX_BACKUPS]
        log.info(f"Retention exceeded. Deleting {len(to_delete)} old folders.")

        for item in to_delete:
            log.info(f"Purging old backup folder: {item['name']}")
            del_req = urllib.request.Request(
                f"https://www.googleapis.com/drive/v3/files/{item['id']}",
                headers={"Authorization": f"Bearer {drive_token}"},
                method="DELETE"
            )
            urllib.request.urlopen(del_req, timeout=30)
    else:
        log.info("Retention limit not reached.")

# Deletes the newly created backup folder if an error occurs
def delete_incomplete_backup():
    if TARGET_FOLDER_ID:
        log.info("Cleaning up incomplete backup folder from Drive...")
        try:
            drive_token = get_access_token(OAUTH_REFRESH_TOKEN_DRIVE)
            del_req = urllib.request.Request(
                f"https://www.googleapis.com/drive/v3/files/{TARGET_FOLDER_ID}",
                headers={"Authorization": f"Bearer {drive_token}"},
                method="DELETE"
            )
            urllib.request.urlopen(del_req, timeout=30)
            log.info("Incomplete backup folder deleted successfully.")
        except Exception as e:
            log.error(f"Failed to delete incomplete backup folder: {e}")

# Starting method
def perform_backup():
    global TARGET_FOLDER_ID
    log.info(f"Backup job started.")
    status = "FAILED"

    try:
        stop_amp_instances()

        TARGET_FOLDER_ID = create_drive_folder(CURRENT_BACKUP_NAME, BASE_FOLDER_ID)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_db = executor.submit(retry_upload, backup_database_stream)
            future_files = executor.submit(retry_upload, backup_files_stream)

            for future in concurrent.futures.as_completed([future_db, future_files]):
                future.result()

        manage_retention()
        status = "SUCCESS"

    except KeyboardInterrupt:
        log.error("Backup failed: Interrupted by user.")
        delete_incomplete_backup()
    except Exception as e:
        log.error(f"Backup failed: {e}")
        delete_incomplete_backup()
    finally:
        upgrade_amp_instances()
        log.info("Backup finished.")
        send_email_report(status)

if __name__ == "__main__":
    if os.geteuid() != 0:
        print("Error: Must run as root.")
        sys.exit(1)
    perform_backup()