#!/usr/bin/env python3
"""Run moon_rs Lua regressions with disposable Docker database services.

From the repository root: python3 assets/test/run_all.py
Docker group newly granted: sg docker -c 'python3 assets/test/run_all.py'
Logs and machine-readable results are written to --output (default /tmp).
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[2]
LOCAL = ["buffer", "json", "seri", "schema", "random", "uuid", "zset", "tobeclose",
         "sharetable", "service", "request_pool", "socket", "socket_frame", "http",
         "httpd", "websocket", "cluster", "cluster_selfconnect", "grpc",
         "grpc/test_grpc_server", "grpc/test_grpc_close", "runtime_libraries", "hotfix", "http_stream", "watchdog"]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def docker(*args, check=True, timeout=60):
    return subprocess.run(["docker", *args], text=True, capture_output=True,
                          check=check, timeout=timeout)


def make_xlsx(path):
    files = {
        "[Content_Types].xml": '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>',
        "_rels/.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
        "xl/workbook.xml": '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>',
        "xl/_rels/workbook.xml.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>',
        "xl/worksheets/sheet1.xml": '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><dimension ref="A1:C2"/><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>name</t></is></c><c r="B1" t="inlineStr"><is><t>number</t></is></c><c r="C1" t="inlineStr"><is><t>enabled</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>中文</t></is></c><c r="B2"><v>42</v></c><c r="C2" t="b"><v>1</v></c></row></sheetData></worksheet>',
    }
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, value in files.items():
            archive.writestr(name, value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", nargs="+", choices=["local", "postgres", "mysql", "mongodb", "redis", "etcd"])
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--output", type=Path, default=Path(tempfile.gettempdir()) / "moon-rs-validation")
    args = parser.parse_args()
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=True)
    selected = set(args.only or ["local", "postgres", "mysql", "mongodb", "redis", "etcd"])
    if not args.skip_build:
        subprocess.run(["cargo", "build", "--workspace", "--all-features", "--locked"], cwd=ROOT, check=True)
    results = {}

    def lua(test, settings=None, timeout=60):
        script = test if "/" in test else "test_" + test
        name = script.replace("/", "_")
        env = os.environ.copy(); env.update(settings or {})
        print("RUN", script, flush=True)
        started = time.monotonic()
        with (output / (name + ".log")).open("w") as log:
            proc = subprocess.Popen([str(ROOT / "target/debug/moon_rs"), str(ROOT / "assets/test" / (script + ".lua"))],
                                    cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL); proc.wait(); code = "TIMEOUT"
        results[script] = {"code": code, "seconds": round(time.monotonic() - started, 2)}
        print("PASS" if code == 0 else "FAIL", script, results[script], flush=True)
        if code != 0:
            print((output / (name + ".log")).read_text()[-6000:], flush=True)

    def service(kind, image, internal, env, command, probe, cases):
        name = "moon-rs-test-" + kind + "-" + uuid.uuid4().hex[:10]
        port = free_port()
        try:
            if docker("image", "inspect", image, check=False).returncode:
                docker("pull", image, timeout=900)
            run = ["run", "-d", "--name", name, "--label", "moon-rs.validation=true", "--memory", "1g",
                   "-p", f"127.0.0.1:{port}:{internal}"]
            for key, value in env.items(): run += ["-e", key + "=" + value]
            docker(*(run + [image] + command))
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                if probe(name): break
                time.sleep(1)
            else: raise RuntimeError(kind + " readiness timed out")
            cases(port)
        except Exception as error:
            # Do not echo environment or connection arguments from failed commands.
            results[kind] = {"code": "ERROR", "error": type(error).__name__}
            print("FAIL", kind, type(error).__name__, flush=True)
        finally:
            docker("rm", "-f", "-v", name, check=False)

    if "local" in selected:
        with tempfile.TemporaryDirectory(prefix="moon-rs-fixtures-") as tmp:
            make_xlsx(Path(tmp) / "sample.xlsx")
            for test in LOCAL: lua(test, {"MOON_TEST_FIXTURES": tmp})
    if selected - {"local"}: docker("info", timeout=15)
    if "postgres" in selected:
        def pg_cases(port):
            env = {"MOON_PG_TEST_URL": f"postgres://postgres:123456@127.0.0.1:{port}/postgres"}
            for test in ["pg", "pg_array", "sqlx_postgres_types", "namesearch"]: lua(test, env, 180)
        service("postgres", "postgres:17", 5432, {"POSTGRES_PASSWORD": "123456"}, [],
                lambda n: docker("exec", n, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", check=False).returncode == 0, pg_cases)
    if "mysql" in selected:
        def mysql_cases(port):
            env = {"MOON_MYSQL_TEST_URL": f"mysql://root:root@127.0.0.1:{port}/mysql?ssl-mode=DISABLED"}
            for test in ["sqlx_mysql_types", "sqlx_stream"]: lua(test, env, 180)
        service("mysql", "mysql:8.4", 3306, {"MYSQL_ROOT_PASSWORD": "root", "MYSQL_ROOT_HOST": "%"}, [],
                lambda n: docker("exec", n, "mysql", "-h", "127.0.0.1", "-uroot", "-proot", "-e", "SELECT 1", check=False).returncode == 0, mysql_cases)
    if "mongodb" in selected:
        def mongo_cases(port):
            env = {"MOON_MONGO_TEST_URL": f"mongodb://root:root@127.0.0.1:{port}/?serverSelectionTimeoutMS=3000"}
            for test in ["mongodb", "mongodb_stream"]: lua(test, env, 180)
        service("mongodb", "mongo:8.0", 27017, {"MONGO_INITDB_ROOT_USERNAME": "root", "MONGO_INITDB_ROOT_PASSWORD": "root"}, [],
                lambda n: docker("exec", n, "mongosh", "--quiet", "--eval", "db.adminCommand('ping').ok", check=False).returncode == 0, mongo_cases)
    if "redis" in selected:
        service("redis", "redis:7-alpine", 6379, {}, ["redis-server", "--save", "", "--appendonly", "no"],
                lambda n: docker("exec", n, "redis-cli", "ping", check=False).stdout.strip() == "PONG",
                lambda port: lua("redis", {"MOON_REDIS_TEST_PORT": str(port)}, 180))
    if "etcd" in selected:
        service("etcd", "quay.io/coreos/etcd:v3.5.17", 2379, {},
                ["/usr/local/bin/etcd", "--listen-client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://127.0.0.1:2379"],
                lambda n: docker("exec", n, "/usr/local/bin/etcdctl", "endpoint", "health", check=False).returncode == 0,
                lambda port: lua("etcd", {"MOON_ETCD_TEST_URL": f"http://127.0.0.1:{port}"}))
    (output / "results.json").write_text(json.dumps(results, indent=2))
    print("RESULTS", json.dumps(results), flush=True)
    return 0 if results and all(row["code"] == 0 for row in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
