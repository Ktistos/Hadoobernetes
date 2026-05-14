"""
tests/test_worker.py
=====================
Unit tests for mapper.py and reducer.py.

Run with:
    pip install pytest pytest-asyncio orjson httpx minio
    pytest tests/test_worker.py -v

All tests mock MinIO and the HTTP client so no live infrastructure is needed.
The tests import mapper and reducer as modules, so the env vars they need at
module-load time must be patched first — handled by the fixtures below.
"""

import asyncio
import hashlib
import io
import json
import os
import sqlite3
import sys
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch, call

import orjson
import pytest

# ---------------------------------------------------------------------------
# Patch all required env vars before importing the worker modules.
# We do this at module scope so that import-time os.environ[] calls succeed.
# ---------------------------------------------------------------------------

MAPPER_ENV = {
    "JOB_MASTER_URL":  "http://job-master-svc:8000",
    "WORKER_ID":       "mapper_0",
    "JOB_ID":          "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    "MAP_ID":          "0",
    "OFFSET_START":    "0",
    "OFFSET_END":      "1000",
    "NUM_REDUCERS":    "2",
    "INPUT_PATH":      "input/data.txt",
    "CODE_PATH":       "code/wordcount.py",
    "MINIO_ENDPOINT":  "minio:9000",
    "MINIO_ACCESS_KEY":"minioadmin",
    "MINIO_SECRET_KEY":"minioadmin",
    "MINIO_BUCKET":    "mapreduce",
    "PING_INTERVAL":   "10",
    "PROFILE":         "0",
}

REDUCER_ENV = {
    "JOB_MASTER_URL":  "http://job-master-svc:8000",
    "WORKER_ID":       "reducer_0",
    "JOB_ID":          "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    "REDUCER_ID":      "0",
    "NUM_MAPPERS":     "2",
    "CODE_PATH":       "code/wordcount.py",
    "OUTPUT_PATH":     "output/part_0.jsonl",
    "MINIO_ENDPOINT":  "minio:9000",
    "MINIO_ACCESS_KEY":"minioadmin",
    "MINIO_SECRET_KEY":"minioadmin",
    "MINIO_BUCKET":    "mapreduce",
    "PING_INTERVAL":   "10",
    "PROFILE":         "0",
}

# Patch env vars at import time
os.environ.update(MAPPER_ENV)
os.environ.update(REDUCER_ENV)

# Add worker directory to sys.path
#sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "worker"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "worker"))

import mapper   # noqa: E402
import reducer  # noqa: E402


# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------

JOB_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
NUM_REDUCERS = 2


def _partition_key(key: str, num_reducers: int = NUM_REDUCERS) -> int:
    """Mirror of mapper.partition_key so tests can predict routing."""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % num_reducers


def _make_input_file(lines: list[str]) -> str:
    """Write lines to a temp file and return its path."""
    fd, path = tempfile.mkstemp(suffix=".txt")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def _make_jsonl_content(pairs: list[tuple]) -> bytes:
    """Encode a list of (key, value) pairs as JSONL bytes."""
    return b"".join(orjson.dumps([k, str(v)]) + b"\n" for k, v in pairs)


def _wordcount_map(key: str, value: str):
    for word in value.lower().split():
        word = word.strip(".,!?;:")
        if word:
            yield word, "1"


def _wordcount_reduce(key: str, values: list):
    yield key, str(len(values))


# ---------------------------------------------------------------------------
# ── mapper.partition_key ────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestPartitionKey:

    def test_deterministic(self):
        """Same key always maps to the same reducer."""
        assert mapper.partition_key("apple") == mapper.partition_key("apple")

    def test_within_range(self):
        """Partition is always in [0, NUM_REDUCERS)."""
        for word in ["apple", "banana", "cherry", "date", "elderberry", ""]:
            r = mapper.partition_key(word)
            assert 0 <= r < NUM_REDUCERS, f"partition out of range for key={word!r}"

    def test_distribution(self):
        """With enough keys, both reducers receive at least one key."""
        words = [f"word_{i}" for i in range(100)]
        buckets = {mapper.partition_key(w) for w in words}
        assert len(buckets) == NUM_REDUCERS, "All keys went to the same reducer"


# ---------------------------------------------------------------------------
# ── mapper._run_sync_core ───────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestRunSyncCore:

    def _make_handles(self):
        """Create in-memory StringIO handles for each reducer partition."""
        handles = {i: io.StringIO() for i in range(NUM_REDUCERS)}
        paths   = {i: f"/tmp/fake_partition_{i}.jsonl" for i in range(NUM_REDUCERS)}
        return handles, paths

    def test_emits_pairs_for_all_lines(self):
        """All lines in the byte range produce map output."""
        lines      = ["hello world", "hello python", "world python"]
        input_path = _make_input_file(lines)
        try:
            handles, paths = self._make_handles()

            # Override OFFSET_END to cover the whole file
            orig_end = mapper.OFFSET_END
            mapper.OFFSET_END = os.path.getsize(input_path)

            pairs_emitted = mapper._run_sync_core(_wordcount_map, input_path, handles)

            mapper.OFFSET_END = orig_end
        finally:
            os.unlink(input_path)

        assert pairs_emitted > 0
        # Each handle contains valid JSONL
        for r_id, fh in handles.items():
            fh.seek(0)
            for line in fh.read().splitlines():
                if line:
                    pair = orjson.loads(line)
                    assert len(pair) == 2, f"Expected [key, value], got {pair}"

    def test_skips_partial_first_line_mid_file(self):
        """When OFFSET_START > 0, the first (partial) line is skipped."""
        lines      = ["line0 word", "line1 word", "line2 word"]
        input_path = _make_input_file(lines)
        try:
            # Find byte position of start of second line
            with open(input_path, "rb") as f:
                first_line_bytes = len(f.readline())

            handles, paths = self._make_handles()

            orig_start = mapper.OFFSET_START
            orig_end   = mapper.OFFSET_END
            mapper.OFFSET_START = first_line_bytes
            mapper.OFFSET_END   = os.path.getsize(input_path)

            pairs_emitted = mapper._run_sync_core(_wordcount_map, input_path, handles)

            mapper.OFFSET_START = orig_start
            mapper.OFFSET_END   = orig_end
        finally:
            os.unlink(input_path)

        # "line0" should not appear in output because it was a partial first line
        all_keys = []
        for fh in handles.values():
            fh.seek(0)
            for line in fh.read().splitlines():
                if line:
                    all_keys.append(orjson.loads(line)[0])

        assert "line0" not in all_keys, "Partial first line should have been skipped"

    def test_empty_lines_are_skipped(self):
        """Blank lines produce no output pairs."""
        lines      = ["", "   ", "hello world", ""]
        input_path = _make_input_file(lines)
        try:
            handles, paths = self._make_handles()

            orig_end       = mapper.OFFSET_END
            mapper.OFFSET_END = os.path.getsize(input_path)

            pairs_emitted  = mapper._run_sync_core(_wordcount_map, input_path, handles)
            mapper.OFFSET_END = orig_end
        finally:
            os.unlink(input_path)

        assert pairs_emitted == 2   # "hello" and "world"

    def test_keys_go_to_correct_partition(self):
        """Every key ends up in the partition file matching partition_key(key)."""
        lines      = ["apple banana cherry"]
        input_path = _make_input_file(lines)
        try:
            handles, paths = self._make_handles()

            orig_end       = mapper.OFFSET_END
            mapper.OFFSET_END = os.path.getsize(input_path)

            mapper._run_sync_core(_wordcount_map, input_path, handles)
            mapper.OFFSET_END = orig_end
        finally:
            os.unlink(input_path)

        for r_id, fh in handles.items():
            fh.seek(0)
            for line in fh.read().splitlines():
                if line:
                    key = orjson.loads(line)[0]
                    expected_partition = _partition_key(key)
                    assert expected_partition == r_id, (
                        f"Key '{key}' ended up in partition {r_id} "
                        f"but should be in {expected_partition}"
                    )

# ---------------------------------------------------------------------------
# ── mapper ping ─────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestMapperPing:

    @pytest.mark.asyncio
    async def test_ping_posts_correct_payload(self):
        """ping() must POST the correct worker_id, worker_type, status."""
        mock_response = MagicMock(status_code=200)
        mock_client   = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.is_closed = False

        # MAPPER_ENV and REDUCER_ENV both set WORKER_ID; the reducer's value
        # wins at module-load time.  Patch the mapper module global directly
        # so this test is independent of env-patching order.
        with patch.object(mapper, "WORKER_ID", "mapper_0"), \
             patch.object(mapper, "_http_client", mock_client):
            await mapper.ping("started")

        mock_client.post.assert_called_once()
        _, kwargs = mock_client.post.call_args
        payload   = kwargs["json"]
        assert payload["worker_id"]   == "mapper_0"
        assert payload["worker_type"] == "mapper"
        assert payload["status"]      == "started"

    @pytest.mark.asyncio
    async def test_ping_does_not_raise_on_network_error(self):
        """A failed ping must be swallowed — the worker must not crash."""
        mock_client      = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("connection refused"))
        mock_client.is_closed = False

        with patch.object(mapper, "_http_client", mock_client):
            # Should not raise
            await mapper.ping("alive")

    @pytest.mark.asyncio
    async def test_ping_sends_all_valid_statuses(self):
        """ping() must accept started, alive, completed, and failed."""
        mock_client      = AsyncMock()
        mock_client.post = AsyncMock(return_value=MagicMock())
        mock_client.is_closed = False

        for status in ("started", "alive", "completed", "failed"):
            with patch.object(mapper, "_http_client", mock_client):
                await mapper.ping(status)

        assert mock_client.post.call_count == 4


# ---------------------------------------------------------------------------
# ── mapper.run() integration ─────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestMapperRunIntegration:

    @pytest.mark.asyncio
    async def test_full_run_uploads_correct_partitions(self):
        """
        Full mapper.run() with mocked MinIO.
        Verifies that:
          - JSONL files are uploaded to the correct reducer-centric paths
          - Output files contain valid [key, value] JSONL lines
        """
        lines      = ["apple banana", "cherry apple", "banana cherry"]
        input_data = ("\n".join(lines) + "\n").encode("utf-8")

        # Capture what was uploaded to MinIO
        uploads: dict[str, bytes] = {}

        def fake_fget_object(bucket, obj_path, local_path):
            if obj_path == "input/data.txt":
                with open(local_path, "wb") as f:
                    f.write(input_data)
            elif obj_path == "code/wordcount.py":
                code = (
                    "def map(key, value):\n"
                    "    for w in value.split():\n"
                    "        yield w, '1'\n"
                )
                with open(local_path, "w") as f:
                    f.write(code)

        def fake_put_object(bucket, path, data, size, content_type=None):
            uploads[path] = data.read()

        mock_minio = MagicMock()
        mock_minio.fget_object.side_effect = fake_fget_object
        mock_minio.put_object.side_effect  = fake_put_object

        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=MagicMock())
        mock_http.is_closed = False

        orig_end       = mapper.OFFSET_END
        mapper.OFFSET_END = len(input_data)

        with patch.object(mapper, "minio_client", mock_minio), \
             patch.object(mapper, "_http_client", mock_http), \
             patch.object(mapper, "OFFSET_START", 0):
            await mapper.run()

        mapper.OFFSET_END = orig_end

        # Check uploads went to reducer-centric paths
        for path in uploads:
            assert f"reducer_" in path, f"Expected reducer-centric path, got: {path}"
            assert f"from_mapper_0.jsonl" in path

        # Verify all uploaded content is valid JSONL with [key, value] arrays
        for path, content in uploads.items():
            for line in content.splitlines():
                if line:
                    pair = orjson.loads(line)
                    assert len(pair) == 2

    @pytest.mark.asyncio
    async def test_run_sends_started_and_completed_pings(self):
        """run() must send 'started' first and 'completed' last."""
        lines      = ["hello world"]
        input_data = ("\n".join(lines) + "\n").encode("utf-8")

        pinged_statuses = []

        def fake_fget_object(bucket, obj_path, local_path):
            if obj_path == "input/data.txt":
                with open(local_path, "wb") as f:
                    f.write(input_data)
            elif obj_path == "code/wordcount.py":
                with open(local_path, "w") as f:
                    f.write("def map(key, value):\n    yield 'k', 'v'\n")

        async def fake_post(url, json=None, timeout=None):
            if "/worker_ping" in url:
                pinged_statuses.append(json["status"])
            return MagicMock()

        mock_minio = MagicMock()
        mock_minio.fget_object.side_effect = fake_fget_object
        mock_minio.put_object = MagicMock()

        mock_http         = AsyncMock()
        mock_http.post    = fake_post
        mock_http.is_closed = False

        orig_end       = mapper.OFFSET_END
        mapper.OFFSET_END = len(input_data)

        with patch.object(mapper, "minio_client", mock_minio), \
             patch.object(mapper, "_http_client", mock_http), \
             patch.object(mapper, "OFFSET_START", 0):
            await mapper.run()

        mapper.OFFSET_END = orig_end

        assert pinged_statuses[0]  == "started",   "First ping must be 'started'"
        assert pinged_statuses[-1] == "completed", "Last ping must be 'completed'"

    @pytest.mark.asyncio
    async def test_run_sends_failed_ping_on_exception(self):
        """If run() raises, it must send a 'failed' ping before re-raising."""
        pinged_statuses = []

        async def fake_post(url, json=None, timeout=None):
            if "/worker_ping" in url:
                pinged_statuses.append(json["status"])
            return MagicMock()

        mock_minio           = MagicMock()
        mock_minio.fget_object.side_effect = RuntimeError("MinIO unavailable")

        mock_http            = AsyncMock()
        mock_http.post       = fake_post
        mock_http.is_closed  = False

        with patch.object(mapper, "minio_client", mock_minio), \
             patch.object(mapper, "_http_client", mock_http):
            with pytest.raises(RuntimeError, match="MinIO unavailable"):
                await mapper.run()

        assert "failed" in pinged_statuses


# ---------------------------------------------------------------------------
# ── reducer._setup_sqlite ───────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestReducerSQLiteSetup:

    def test_creates_map_data_table(self):
        """_setup_sqlite must create map_data table with key and value columns."""
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            conn, cursor = reducer._setup_sqlite(db_path)
            # Insert a row and read it back
            cursor.execute("INSERT INTO map_data (key, value) VALUES ('k', 'v')")
            conn.commit()
            row = cursor.execute("SELECT key, value FROM map_data").fetchone()
            assert row == ("k", "v")
            conn.close()
        finally:
            os.unlink(db_path)

    def test_pragmas_set(self):
        """The speed-PRAGMAs must be accepted without error."""
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            # If the PRAGMAs caused an error, _setup_sqlite would raise
            conn, _ = reducer._setup_sqlite(db_path)
            synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]
            # 0 = OFF
            assert synchronous == 0
            conn.close()
        finally:
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# ── reducer._ingest_partition ────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestReducerIngestPartition:

    def _make_db(self) -> tuple[sqlite3.Connection, sqlite3.Cursor, str]:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        conn, cursor = reducer._setup_sqlite(path)
        return conn, cursor, path

    def test_ingests_all_rows(self):
        """All JSONL lines from the stream are inserted into SQLite."""
        pairs   = [("apple", "1"), ("apple", "1"), ("banana", "1")]
        content = _make_jsonl_content(pairs)

        conn, cursor, db_path = self._make_db()
        try:
            stream = io.BytesIO(content)
            rows   = reducer._ingest_partition(cursor, stream)
            conn.commit()

            assert rows == 3
            db_rows = cursor.execute(
                "SELECT key, value FROM map_data ORDER BY key"
            ).fetchall()
            assert db_rows == [("apple", "1"), ("apple", "1"), ("banana", "1")]
        finally:
            conn.close()
            os.unlink(db_path)

    def test_skips_empty_lines(self):
        """Empty lines in the JSONL stream must not cause errors or empty rows."""
        content = b'["apple","1"]\n\n["banana","1"]\n'

        conn, cursor, db_path = self._make_db()
        try:
            stream = io.BytesIO(content)
            rows   = reducer._ingest_partition(cursor, stream)
            conn.commit()

            assert rows == 2
        finally:
            conn.close()
            os.unlink(db_path)

    def test_batch_insert_large_dataset(self):
        """Datasets larger than SQLITE_BATCH_SIZE are fully inserted."""
        n_rows  = reducer.SQLITE_BATCH_SIZE * 3 + 17
        pairs   = [(f"key_{i:06d}", "1") for i in range(n_rows)]
        content = _make_jsonl_content(pairs)

        conn, cursor, db_path = self._make_db()
        try:
            stream = io.BytesIO(content)
            rows   = reducer._ingest_partition(cursor, stream)
            conn.commit()

            assert rows == n_rows
            count = cursor.execute("SELECT COUNT(*) FROM map_data").fetchone()[0]
            assert count == n_rows
        finally:
            conn.close()
            os.unlink(db_path)

# ---------------------------------------------------------------------------
# ── reducer._run_reduce_phase ────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestReducerRunReducePhase:

    def _setup_db_with_data(self, pairs: list[tuple]) -> tuple:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        conn, cursor = reducer._setup_sqlite(db_path)
        cursor.executemany(
            "INSERT INTO map_data (key, value) VALUES (?, ?)", pairs
        )
        conn.commit()
        return conn, cursor, db_path

    def test_wordcount_reduce(self):
        """WordCount reduce: each key should have count equal to len(values)."""
        pairs   = [("apple", "1"), ("apple", "1"), ("banana", "1")]
        conn, _, db_path = self._setup_db_with_data(pairs)

        out_buf = io.BytesIO()
        result_count = reducer._run_reduce_phase(conn, _wordcount_reduce, out_buf)
        conn.close()
        os.unlink(db_path)

        assert result_count == 2   # "apple" and "banana"

        out_buf.seek(0)
        results = {}
        for line in out_buf.read().splitlines():
            if line:
                k, v = orjson.loads(line)
                results[k] = v

        assert results["apple"]  == "2"
        assert results["banana"] == "1"

    def test_output_is_sorted_by_key(self):
        """Output keys must arrive in alphabetical order."""
        pairs   = [("zebra","1"), ("apple","1"), ("mango","1")]
        conn, _, db_path = self._setup_db_with_data(pairs)

        out_buf = io.BytesIO()
        reducer._run_reduce_phase(conn, _wordcount_reduce, out_buf)
        conn.close()
        os.unlink(db_path)

        out_buf.seek(0)
        keys = [orjson.loads(line)[0] for line in out_buf.read().splitlines() if line]
        assert keys == sorted(keys), f"Output not sorted: {keys}"

    def test_empty_database_produces_no_output(self):
        """If no data was ingested, reduce phase writes nothing."""
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        conn, _ = reducer._setup_sqlite(db_path)

        out_buf = io.BytesIO()
        result_count = reducer._run_reduce_phase(conn, _wordcount_reduce, out_buf)
        conn.close()
        os.unlink(db_path)

        assert result_count == 0
        assert out_buf.getvalue() == b""

    def test_hot_key_with_many_values(self):
        """A key with a very large number of values must reduce correctly."""
        n      = 10_000
        pairs  = [("hotkey", "1")] * n
        conn, _, db_path = self._setup_db_with_data(pairs)

        out_buf = io.BytesIO()
        reducer._run_reduce_phase(conn, _wordcount_reduce, out_buf)
        conn.close()
        os.unlink(db_path)

        out_buf.seek(0)
        results = {}
        for line in out_buf.read().splitlines():
            if line:
                k, v = orjson.loads(line)
                results[k] = v

        assert results["hotkey"] == str(n)

# ---------------------------------------------------------------------------
# ── reducer ping ────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestReducerPing:

    @pytest.mark.asyncio
    async def test_ping_posts_correct_payload(self):
        """ping() must POST the correct worker_id, worker_type, status."""
        mock_client      = AsyncMock()
        mock_client.post = AsyncMock(return_value=MagicMock())
        mock_client.is_closed = False

        with patch.object(reducer, "_http_client", mock_client):
            await reducer.ping("completed")

        _, kwargs = mock_client.post.call_args
        payload   = kwargs["json"]
        assert payload["worker_id"]   == "reducer_0"
        assert payload["worker_type"] == "reducer"
        assert payload["status"]      == "completed"

    @pytest.mark.asyncio
    async def test_ping_does_not_raise_on_network_error(self):
        """A failed ping must not crash the reducer."""
        mock_client       = AsyncMock()
        mock_client.post  = AsyncMock(side_effect=Exception("timeout"))
        mock_client.is_closed = False

        with patch.object(reducer, "_http_client", mock_client):
            await reducer.ping("alive")  # must not raise


# ---------------------------------------------------------------------------
# ── reducer.run() integration ────────────────────────────────────────────────
# ---------------------------------------------------------------------------

class TestReducerRunIntegration:

    @pytest.mark.asyncio
    async def test_full_run_produces_correct_wordcount_output(self):
        """
        Full reducer.run() with mocked MinIO list_objects + get_object.
        Verifies the correct wordcount output is uploaded to OUTPUT_PATH.
        """
        # Simulate two mapper partition files for reducer_0
        partition_0 = _make_jsonl_content([("apple","1"),("apple","1"),("banana","1")])
        partition_1 = _make_jsonl_content([("apple","1"),("cherry","1")])

        partitions = {
            f"intermediate/{JOB_ID}/reducer_0/from_mapper_0.jsonl": partition_0,
            f"intermediate/{JOB_ID}/reducer_0/from_mapper_1.jsonl": partition_1,
        }

        uploaded: dict[str, bytes] = {}

        def fake_list_objects(bucket, prefix=None, recursive=False):
            return [
                MagicMock(object_name=name)
                for name in partitions
                if name.startswith(prefix or "")
            ]

        def fake_get_object(bucket, path):
            content  = partitions[path]
            response = io.BytesIO(content)
            response.close        = lambda: None
            response.release_conn = lambda: None
            return response

        def fake_fget_object(bucket, obj_path, local_path):
            assert obj_path == "code/wordcount.py"
            code = (
                "def reduce(key, values):\n"
                "    yield key, str(len(values))\n"
            )
            with open(local_path, "w") as f:
                f.write(code)

        def fake_put_object(bucket, path, data, size, content_type=None):
            uploaded[path] = data.read()

        mock_minio = MagicMock()
        mock_minio.list_objects.side_effect = fake_list_objects
        mock_minio.get_object.side_effect   = fake_get_object
        mock_minio.fget_object.side_effect  = fake_fget_object
        mock_minio.put_object.side_effect   = fake_put_object

        mock_http         = AsyncMock()
        mock_http.post    = AsyncMock(return_value=MagicMock())
        mock_http.is_closed = False

        with patch.object(reducer, "minio_client", mock_minio), \
             patch.object(reducer, "_http_client", mock_http):
            await reducer.run()

        # Verify output was uploaded to OUTPUT_PATH
        assert "output/part_0.jsonl" in uploaded

        results = {}
        for line in uploaded["output/part_0.jsonl"].splitlines():
            if line:
                k, v = orjson.loads(line)
                results[k] = int(v)

        assert results["apple"]  == 3
        assert results["banana"] == 1
        assert results["cherry"] == 1

    @pytest.mark.asyncio
    async def test_run_sends_started_and_completed_pings(self):
        """reducer.run() must send 'started' first and 'completed' last."""
        partitions = {
            f"intermediate/{JOB_ID}/reducer_0/from_mapper_0.jsonl":
                _make_jsonl_content([("k", "v")])
        }
        pinged_statuses = []

        def fake_list_objects(bucket, prefix=None, recursive=False):
            return [MagicMock(object_name=n) for n in partitions if n.startswith(prefix or "")]

        def fake_get_object(bucket, path):
            r = io.BytesIO(partitions[path])
            r.close = lambda: None
            r.release_conn = lambda: None
            return r

        def fake_fget_object(bucket, path, local):
            with open(local, "w") as f:
                f.write("def reduce(key, values):\n    yield key, str(len(values))\n")

        async def fake_post(url, json=None, timeout=None):
            if "/worker_ping" in url:
                pinged_statuses.append(json["status"])
            return MagicMock()

        mock_minio = MagicMock()
        mock_minio.list_objects.side_effect = fake_list_objects
        mock_minio.get_object.side_effect   = fake_get_object
        mock_minio.fget_object.side_effect  = fake_fget_object
        mock_minio.put_object               = MagicMock()

        mock_http         = AsyncMock()
        mock_http.post    = fake_post
        mock_http.is_closed = False

        with patch.object(reducer, "minio_client", mock_minio), \
             patch.object(reducer, "_http_client", mock_http):
            await reducer.run()

        assert pinged_statuses[0]  == "started",   "First ping must be 'started'"
        assert pinged_statuses[-1] == "completed", "Last ping must be 'completed'"

    @pytest.mark.asyncio
    async def test_run_sends_failed_ping_on_exception(self):
        """If run() raises, it must send 'failed' before re-raising."""
        pinged_statuses = []

        async def fake_post(url, json=None, timeout=None):
            if "/worker_ping" in url:
                pinged_statuses.append(json["status"])
            return MagicMock()

        mock_minio       = MagicMock()
        mock_minio.fget_object.side_effect = RuntimeError("code download failed")

        mock_http        = AsyncMock()
        mock_http.post   = fake_post
        mock_http.is_closed = False

        with patch.object(reducer, "minio_client", mock_minio), \
             patch.object(reducer, "_http_client", mock_http):
            with pytest.raises(RuntimeError, match="code download failed"):
                await reducer.run()

        assert "failed" in pinged_statuses

    @pytest.mark.asyncio
    async def test_run_handles_no_partition_files(self):
        """If no mapper produced output for this reducer, run() must still complete."""

        def fake_list_objects(bucket, prefix=None, recursive=False):
            return []   # No files at all

        def fake_fget_object(bucket, path, local):
            with open(local, "w") as f:
                f.write("def reduce(key, values):\n    yield key, str(len(values))\n")

        mock_minio = MagicMock()
        mock_minio.list_objects.side_effect = fake_list_objects
        mock_minio.fget_object.side_effect  = fake_fget_object
        mock_minio.put_object               = MagicMock()

        mock_http         = AsyncMock()
        mock_http.post    = AsyncMock(return_value=MagicMock())
        mock_http.is_closed = False

        with patch.object(reducer, "minio_client", mock_minio), \
             patch.object(reducer, "_http_client", mock_http):
            # Should complete without raising
            await reducer.run()

        # Verify an empty output file was uploaded
        mock_minio.put_object.assert_called_once()
