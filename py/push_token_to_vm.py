import subprocess
import shlex
import sys
import time
import os
from pathlib import Path

VM_HOST = "23.92.19.61"
VM_USER = "root"
ENV_PATH = "/etc/lucidDreamBaseball.env"
MAX_ATTEMPTS = 3


def _is_wsl() -> bool:
    if os.environ.get("WSL_DISTRO_NAME"):
        return True
    proc_version = Path("/proc/version")
    try:
        return "microsoft" in proc_version.read_text(encoding="utf-8", errors="ignore").lower()
    except OSError:
        return False


def _wsl_windows_ed25519_candidates() -> list[Path]:
    """Under WSL, the key that works with ssh.exe is often under /mnt/c/Users/<win>/.ssh/."""
    users = Path("/mnt/c/Users")
    if not users.is_dir():
        return []
    skip = {"Public", "Default", "All Users", "Default User"}
    found: list[Path] = []
    for p in sorted(users.iterdir()):
        if not p.is_dir() or p.name in skip:
            continue
        key = p / ".ssh" / "id_ed25519"
        if key.is_file():
            found.append(key)
    return found


def resolve_ssh_key_path() -> str:
    """
    Pick the private key for ssh -i.

    - LDB_SSH_KEY_PATH: explicit path (highest priority if file exists).
    - WSL: prefer the Windows user profile key (same as PowerShell ssh -i)
      when unambiguous; ~/.ssh may be a different keypair.
    - ~/.ssh/id_ed25519 when present (native Linux/macOS/Windows Python).
    """
    override = os.environ.get("LDB_SSH_KEY_PATH")
    if override:
        p = Path(override).expanduser()
        if not p.is_file():
            raise RuntimeError(
                f"LDB_SSH_KEY_PATH is set but not a file: {p}. "
                "Unset it or point it at your id_ed25519 private key."
            )
        return str(p)

    home_key = Path.home() / ".ssh" / "id_ed25519"

    if _is_wsl():
        win_user = os.environ.get("LDB_WINDOWS_USERNAME", "").strip()
        if win_user:
            win_key = Path("/mnt/c/Users") / win_user / ".ssh" / "id_ed25519"
            if win_key.is_file():
                return str(win_key)
            raise RuntimeError(
                f"LDB_WINDOWS_USERNAME={win_user!r} but no file: {win_key}"
            )

        wsl_keys = _wsl_windows_ed25519_candidates()
        if len(wsl_keys) == 1:
            return str(wsl_keys[0])
        if len(wsl_keys) > 1:
            pretty = ", ".join(str(k) for k in wsl_keys)
            raise RuntimeError(
                "Multiple Windows id_ed25519 keys found under /mnt/c/Users. "
                f"Set LDB_SSH_KEY_PATH or LDB_WINDOWS_USERNAME. Candidates: {pretty}"
            )

    if home_key.is_file():
        return str(home_key)

    if _is_wsl():
        raise RuntimeError(
            "No id_ed25519 private key found. Under WSL, install/copy your key to "
            "~/.ssh/id_ed25519 or set LDB_SSH_KEY_PATH to the full path of the key "
            "that works with ssh (e.g. /mnt/c/Users/You/.ssh/id_ed25519)."
        )

    raise RuntimeError(
        "No id_ed25519 private key found at ~/.ssh/id_ed25519. "
        "Set LDB_SSH_KEY_PATH to the full path of the key that works with ssh."
    )


def _ssh_key_on_wsl_drvfs(ssh_key_path: str) -> bool:
    """True if the key lives on a /mnt/<drive>/ mount (9p/drvfs); Linux ssh rejects 0777 perms there."""
    p = ssh_key_path.replace("\\", "/")
    return p.startswith("/mnt/") and len(p) > 5


def _wsl_path_to_windows(path: str) -> str:
    """Convert a WSL path to a Windows path for use with ssh.exe (e.g. C:\\Users\\...)."""
    try:
        r = subprocess.run(
            ["wslpath", "-w", path],
            capture_output=True,
            text=True,
            check=True,
        )
        return r.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        p = path.replace("\\", "/")
        if not p.startswith("/mnt/"):
            return path
        parts = [x for x in p.split("/") if x]
        if len(parts) < 3 or parts[0] != "mnt":
            return path
        drive = parts[1].upper()
        rest = "\\".join(parts[2:])
        return f"{drive}:\\{rest}"


def _windows_openssh_ssh_exe() -> Path:
    exe = Path("/mnt/c/Windows/System32/OpenSSH/ssh.exe")
    if exe.is_file():
        return exe
    raise RuntimeError(
        "WSL: expected Windows OpenSSH at /mnt/c/Windows/System32/OpenSSH/ssh.exe "
        "to avoid drvfs private-key permission errors. Install OpenSSH Client on Windows, "
        "or copy id_ed25519 to ~/.ssh/ in WSL and chmod 600, then set LDB_SSH_KEY_PATH."
    )


def build_ssh_command(ssh_key_path: str, remote_cmd: str) -> list[str]:
    """
    Use Windows ssh.exe from WSL when the key is on /mnt/c/... — Linux ssh refuses
    'too open' keys on drvfs (often 0777). Set LDB_USE_WINDOWS_SSH=0 to disable and
    use a key under the Linux filesystem instead (~/.ssh with chmod 600).
    """
    use_win = os.environ.get("LDB_USE_WINDOWS_SSH", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )
    if (
        use_win
        and _is_wsl()
        and _ssh_key_on_wsl_drvfs(ssh_key_path)
    ):
        win_key = _wsl_path_to_windows(ssh_key_path)
        win_ssh = _windows_openssh_ssh_exe()
        return [
            str(win_ssh),
            "-i",
            win_key,
            "-o",
            "IdentitiesOnly=yes",
            f"{VM_USER}@{VM_HOST}",
            remote_cmd,
        ]
    return [
        "ssh",
        "-i",
        ssh_key_path,
        "-o",
        "IdentitiesOnly=yes",
        f"{VM_USER}@{VM_HOST}",
        remote_cmd,
    ]


def get_token():
    sys.path.insert(0, "py")
    from ingestion.auth import AuthManager
    session = AuthManager().get_session(force_refresh=True, dry_run=False, skip_auth=False)
    if not session.api_token:
        raise RuntimeError("No API token extracted from local auth flow.")
    return session.api_token


def push_token(token):
    remote_cmd = (
        f"sudo sed -i '/^CBS_API_TOKEN=/d' {shlex.quote(ENV_PATH)} && "
        f"echo CBS_API_TOKEN={shlex.quote(token)} | sudo tee -a {shlex.quote(ENV_PATH)} >/dev/null && "
        f"sudo chmod 600 {shlex.quote(ENV_PATH)}"
    )
    ssh_key_path = resolve_ssh_key_path()
    ssh_cmd = build_ssh_command(ssh_key_path, remote_cmd)
    result = subprocess.run(ssh_cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        details = stderr or stdout or "no stderr/stdout from ssh"
        raise RuntimeError(f"SSH command failed ({result.returncode}): {details}")


def main():
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            token = get_token()
            push_token(token)
            print("Token pushed to VM successfully.")
            return
        except Exception as exc:
            last_error = exc
            print(f"Attempt {attempt}/{MAX_ATTEMPTS} failed: {exc}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(5 * attempt)
    raise SystemExit(f"Token push failed after {MAX_ATTEMPTS} attempts: {last_error}")


if __name__ == "__main__":
    main()
