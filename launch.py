"""
launch.py
=========
Launcher for the AWS Sales ETL Pipeline Dashboard.

- Starts Streamlit on port 8501 (binds all interfaces)
- Opens a FREE public HTTPS tunnel via localhost.run (no signup needed)
- Prints local + public URLs so you can share with anyone

Usage:
    python launch.py                # start with public URL
    python launch.py --no-tunnel    # local only
    python launch.py --port 8502    # custom port
"""

import argparse
import os
import re
import subprocess
import sys
import threading
import time
import webbrowser

# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Launch AWS Sales ETL Dashboard")
    p.add_argument("--port",      type=int, default=8501, help="Streamlit port (default 8501)")
    p.add_argument("--no-tunnel", action="store_true",    help="Skip public tunnel")
    return p.parse_args()


def check_app_file():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    if not os.path.exists("app.py"):
        print("[ERROR] app.py not found. Run this from the project root.")
        sys.exit(1)


def start_streamlit(port: int) -> subprocess.Popen:
    cmd = [
        sys.executable, "-m", "streamlit", "run", "app.py",
        f"--server.port={port}",
        "--server.address=0.0.0.0",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]
    print(f"\n[1/3] Starting Streamlit on port {port}...")
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(5)
    if proc.poll() is not None:
        print("[ERROR] Streamlit failed to start. Check that 'streamlit' is installed.")
        sys.exit(1)
    print(f"      Streamlit is up at http://localhost:{port}")
    return proc


def start_tunnel(port: int) -> tuple:
    """
    Opens an SSH reverse tunnel via localhost.run (free, no account).
    Returns (proc, public_url).
    """
    print("[2/3] Opening public tunnel via localhost.run ...")
    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=5",
        "-R", f"80:localhost:{port}",
        "nokey@localhost.run",
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    public_url = ""
    url_re = re.compile(r"https://[a-zA-Z0-9]+\.lhr\.life")
    deadline = time.time() + 20
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        m = url_re.search(line)
        if m:
            public_url = m.group(0)
            break

    # Drain remaining output in a background thread so SSH stays alive
    def _drain(p):
        for _ in p.stdout:
            pass

    threading.Thread(target=_drain, args=(proc,), daemon=True).start()

    if not public_url:
        print("[WARN] Could not capture public URL from localhost.run.")
        print("       Try running manually:")
        print(f"       ssh -R 80:localhost:{port} nokey@localhost.run")

    return proc, public_url


def print_banner(port: int, public_url: str):
    local_url = f"http://localhost:{port}"
    print("\n" + "=" * 62)
    print("  AWS Sales ETL Pipeline - RUNNING")
    print("=" * 62)
    print(f"  Local URL  :  {local_url}")
    if public_url:
        print(f"  Public URL :  {public_url}")
        print()
        print("  Share the Public URL with anyone!")
        print("  (It stays active while this script is running.)")
    else:
        print()
        print("  No public tunnel active.")
        print("  To get a public URL run:")
        print(f"    ssh -R 80:localhost:{port} nokey@localhost.run")
    print("=" * 62)
    print("  Press Ctrl+C to stop everything.")
    print("=" * 62 + "\n")


def main():
    args = parse_args()
    check_app_file()

    streamlit_proc = start_streamlit(args.port)

    tunnel_proc = None
    public_url = ""
    if not args.no_tunnel:
        tunnel_proc, public_url = start_tunnel(args.port)

    print_banner(args.port, public_url)

    try:
        webbrowser.open(f"http://localhost:{args.port}")
    except Exception:
        pass

    try:
        while True:
            if streamlit_proc.poll() is not None:
                print("[ERROR] Streamlit exited unexpectedly.")
                break
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down...")
    finally:
        streamlit_proc.terminate()
        if tunnel_proc:
            tunnel_proc.terminate()
        print("[INFO] Done.")


if __name__ == "__main__":
    main()
