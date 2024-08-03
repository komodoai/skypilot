"""Global user state, backed by a sqlite database.

Concepts:
- Cluster name: a user-supplied or auto-generated unique name to identify a
  cluster.
- Cluster handle: (non-user facing) an opaque backend handle for us to
  interact with a cluster.
"""
import json
import os
import pathlib
import pickle
import sqlite3
import time
import typing
from typing import Any, Dict, List, Optional, Set, Tuple
import uuid
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from sky import clouds
from sky import status_lib
from sky.constants import SKY_HOME
from sky.utils import common_utils
from sky.utils import db_utils

if typing.TYPE_CHECKING:
    from sky import backends
    from sky.data import Storage

from sqlalchemy import create_engine, Table, MetaData, select, and_, literal, update, delete, join, outerjoin, case
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.sql import text

_ENABLED_CLOUDS_KEY = 'enabled_clouds'


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_komodo_db_resources():
    if os.environ.get('DATABASE_URL') is None:
        print("No DATABASE_URL found in the environment, running against local sqlite db")
        return None, None, None, None, None
    print(f"Running against a Komodo db")
    # Create SQLAlchemy engine
    engine = create_engine(os.environ['DATABASE_URL'], pool_pre_ping=True)
    # Create MetaData instance
    metadata = MetaData()
    # Reflect tables from the database
    clusters = Table('clusters', metadata, autoload_with=engine)
    cluster_history = Table('cluster_history', metadata, autoload_with=engine)
    config = Table('config', metadata, autoload_with=engine)
    storage = Table('storage', metadata, autoload_with=engine)

    return engine, clusters, cluster_history, config, storage

if os.environ.get('DATABASE_URL', None):
    engine, clusters, cluster_history, config, storage = get_komodo_db_resources()
else:
    engine = None
    _DB_PATH = os.path.expanduser(f'{SKY_HOME}/state.db')
    pathlib.Path(_DB_PATH).parents[0].mkdir(parents=True, exist_ok=True)

def _get_user_id():
    user_id = os.environ['KOMODO_USER_ID']
    return user_id

def _parse_name_values(full_name: str | None = None):
  cluster_id = full_name # os.environ.get("KOMODO_CLUSTER_ID")
  name = os.environ.get('KOMODO_CLUSTER_NAME')

  return cluster_id, name


def create_table(cursor, conn):
    # Enable WAL mode to avoid locking issues.
    # See: issue #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if not common_utils.is_wsl():
        try:
            cursor.execute('PRAGMA journal_mode=WAL')
        except sqlite3.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    # Table for Clusters
    cursor.execute("""\
        CREATE TABLE IF NOT EXISTS clusters (
        name TEXT PRIMARY KEY,
        launched_at INTEGER,
        handle BLOB,
        last_use TEXT,
        status TEXT,
        autostop INTEGER DEFAULT -1,
        metadata TEXT DEFAULT "{}",
        to_down INTEGER DEFAULT 0,
        owner TEXT DEFAULT null,
        cluster_hash TEXT DEFAULT null,
        storage_mounts_metadata BLOB DEFAULT null,
        cluster_ever_up INTEGER DEFAULT 0)""")

    # Table for Cluster History
    # usage_intervals: List[Tuple[int, int]]
    #  Specifies start and end timestamps of cluster.
    #  When the last end time is None, the cluster is still UP.
    #  Example: [(start1, end1), (start2, end2), (start3, None)]

    # requested_resources: Set[resource_lib.Resource]
    #  Requested resources fetched from task that user specifies.

    # launched_resources: Optional[resources_lib.Resources]
    #  Actual launched resources fetched from handle for cluster.

    # num_nodes: Optional[int] number of nodes launched.

    cursor.execute("""\
        CREATE TABLE IF NOT EXISTS cluster_history (
        cluster_hash TEXT PRIMARY KEY,
        name TEXT,
        num_nodes int,
        requested_resources BLOB,
        launched_resources BLOB,
        usage_intervals BLOB)""")
    # Table for configs (e.g. enabled clouds)
    cursor.execute("""\
        CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY, value TEXT)""")
    # Table for Storage
    cursor.execute("""\
        CREATE TABLE IF NOT EXISTS storage (
        name TEXT PRIMARY KEY,
        launched_at INTEGER,
        handle BLOB,
        last_use TEXT,
        status TEXT)""")
    # For backward compatibility.
    # TODO(zhwu): Remove this function after all users have migrated to
    # the latest version of SkyPilot.
    # Add autostop column to clusters table
    db_utils.add_column_to_table(cursor, conn, 'clusters', 'autostop',
                                 'INTEGER DEFAULT -1')

    db_utils.add_column_to_table(cursor, conn, 'clusters', 'metadata',
                                 'TEXT DEFAULT "{}"')

    db_utils.add_column_to_table(cursor, conn, 'clusters', 'to_down',
                                 'INTEGER DEFAULT 0')

    db_utils.add_column_to_table(cursor, conn, 'clusters', 'owner', 'TEXT')

    db_utils.add_column_to_table(cursor, conn, 'clusters', 'cluster_hash',
                                 'TEXT DEFAULT null')

    db_utils.add_column_to_table(cursor, conn, 'clusters',
                                 'storage_mounts_metadata', 'BLOB DEFAULT null')
    db_utils.add_column_to_table(
        cursor,
        conn,
        'clusters',
        'cluster_ever_up',
        'INTEGER DEFAULT 0',
        # Set the value to 1 so that all the existing clusters before #2977
        # are considered as ever up, i.e:
        #   existing cluster's default (null) -> 1;
        #   new cluster's default -> 0;
        # This is conservative for the existing clusters: even if some INIT
        # clusters were never really UP, setting it to 1 means they won't be
        # auto-deleted during any failover.
        value_to_replace_existing_entries=1)
    conn.commit()

if engine is None:
    _DB = db_utils.SQLiteConn(_DB_PATH, create_table)


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def add_or_update_cluster(cluster_name: str,
                          cluster_handle: 'backends.ResourceHandle',
                          requested_resources: Optional[Set[Any]],
                          ready: bool,
                          is_launch: bool = True):
    """Adds or updates cluster_name -> cluster_handle mapping.

    Args:
        cluster_name: Name of the cluster.
        cluster_handle: backends.ResourceHandle of the cluster.
        requested_resources: Resources requested for cluster.
        ready: Whether the cluster is ready to use. If False, the cluster will
            be marked as INIT, otherwise it will be marked as UP.
        is_launch: if the cluster is firstly launched. If True, the launched_at
            and last_use will be updated. Otherwise, use the old value.
    """
    # FIXME: launched_at will be changed when `sky launch -c` is called.
    handle = pickle.dumps(cluster_handle)
    cluster_launched_at = int(time.time()) if is_launch else None
    last_use = common_utils.get_pretty_entry_point() if is_launch else None
    status = status_lib.ClusterStatus.INIT
    if ready:
        status = status_lib.ClusterStatus.UP

    # TODO (sumanth): Cluster history table will have multiple entries
    # when the cluster failover through multiple regions (one entry per region).
    # It can be more inaccurate for the multi-node cluster
    # as the failover can have the nodes partially UP.
    cluster_hash = _get_hash_for_existing_cluster(cluster_name) or str(
        uuid.uuid4())
    usage_intervals = _get_cluster_usage_intervals(cluster_hash)

    # first time a cluster is being launched
    if not usage_intervals:
        usage_intervals = []

    # if this is the cluster init or we are starting after a stop
    if not usage_intervals or usage_intervals[-1][-1] is not None:
        if cluster_launched_at is None:
            # This could happen when the cluster is restarted manually on the
            # cloud console. In this case, we will use the current time as the
            # cluster launched time.
            # TODO(zhwu): We should use the time when the cluster is restarted
            # to be more accurate.
            cluster_launched_at = int(time.time())
        usage_intervals.append((cluster_launched_at, None))

    if engine:
        with engine.connect() as connection:
            # Insert or replace logic
            cluster_id, cluster_name = _parse_name_values(cluster_name)
            user_id = _get_user_id()
            stmt = insert(clusters).values(
                id=cluster_id,
                name=cluster_name,
                launched_at=cluster_launched_at,
                handle=handle,
                last_use=last_use,
                status=status.value,
                autostop=-1,  # Default value, will be overridden if conflict
                to_down=0,    # Default value, will be overridden if conflict
                metadata="{}", # Default value, will be overridden if conflict
                owner=None,    # Default value, will be overridden if conflict
                cluster_hash=cluster_hash,
                storage_mounts_metadata=None,  # Default value, will be overridden if conflict
                cluster_ever_up=int(ready),
                user_id=user_id
            )

            autostop_subquery = select(clusters.c.autostop).where(
                and_(clusters.c.id == cluster_id, clusters.c.status != status_lib.ClusterStatus.STOPPED.value)
            ).scalar_subquery()

            to_down_subquery = select(clusters.c.to_down).where(
                and_(clusters.c.id == cluster_id, clusters.c.status != status_lib.ClusterStatus.STOPPED.value)
            ).scalar_subquery()

            metadata_subquery = select(clusters.c.metadata).where(clusters.c.id == cluster_id).scalar_subquery()

            owner_subquery = select(clusters.c.owner).where(clusters.c.id == cluster_id).scalar_subquery()

            storage_mounts_metadata_subquery = select(clusters.c.storage_mounts_metadata).where(clusters.c.id == cluster_id).scalar_subquery()

            cluster_ever_up_subquery = select(clusters.c.cluster_ever_up).where(clusters.c.id == cluster_id).scalar_subquery()

            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'launched_at': literal(cluster_launched_at).label('launched_at') if cluster_launched_at is not None else select(clusters.c.launched_at).where(clusters.c.id == cluster_id).scalar_subquery(),
                    'handle': stmt.excluded.handle,
                    'last_use': literal(last_use).label('last_use') if last_use is not None else select(clusters.c.last_use).where(clusters.c.id == cluster_id).scalar_subquery(),
                    'status': stmt.excluded.status,
                    'autostop': autostop_subquery if autostop_subquery is not None else literal(-1),
                    'to_down': to_down_subquery if to_down_subquery is not None else literal(0),
                    'metadata': metadata_subquery if metadata_subquery is not None else literal("{}"),
                    'owner': owner_subquery if owner_subquery is not None else literal(None),
                    'cluster_hash': stmt.excluded.cluster_hash,
                    'storage_mounts_metadata': storage_mounts_metadata_subquery if storage_mounts_metadata_subquery is not None else literal(None),
                    'cluster_ever_up': case(
                        (cluster_ever_up_subquery.is_(None), literal(int(ready))),
                        (literal(int(ready)).is_(None), cluster_ever_up_subquery)).label('cluster_ever_up'),
                        # else_=(cluster_ever_up_subquery | literal(int(ready)))).label('cluster_ever_up')
                }
            )
            connection.execute(on_conflict_stmt)
            connection.commit()
    else:
        _DB.cursor.execute(
            'INSERT or REPLACE INTO clusters'
            # All the fields need to exist here, even if they don't need
            # be changed, as the INSERT OR REPLACE statement will replace
            # the field of the existing row with the default value if not
            # specified.
            '(name, launched_at, handle, last_use, status, '
            'autostop, to_down, metadata, owner, cluster_hash, '
            'storage_mounts_metadata, cluster_ever_up) '
            'VALUES ('
            # name
            '?, '
            # launched_at
            'COALESCE('
            '?, (SELECT launched_at FROM clusters WHERE name=?)), '
            # handle
            '?, '
            # last_use
            'COALESCE('
            '?, (SELECT last_use FROM clusters WHERE name=?)), '
            # status
            '?, '
            # autostop
            # Keep the old autostop value if it exists, otherwise set it to
            # default -1.
            'COALESCE('
            '(SELECT autostop FROM clusters WHERE name=? AND status!=?), -1), '
            # Keep the old to_down value if it exists, otherwise set it to
            # default 0.
            'COALESCE('
            '(SELECT to_down FROM clusters WHERE name=? AND status!=?), 0),'
            # Keep the old metadata value if it exists, otherwise set it to
            # default {}.
            'COALESCE('
            '(SELECT metadata FROM clusters WHERE name=?), "{}"),'
            # Keep the old owner value if it exists, otherwise set it to
            # default null.
            'COALESCE('
            '(SELECT owner FROM clusters WHERE name=?), null),'
            # cluster_hash
            '?,'
            # storage_mounts_metadata
            'COALESCE('
            '(SELECT storage_mounts_metadata FROM clusters WHERE name=?), null), '
            # cluster_ever_up
            '((SELECT cluster_ever_up FROM clusters WHERE name=?) OR ?)'
            ')',
            (
                # name
                cluster_name,
                # launched_at
                cluster_launched_at,
                cluster_name,
                # handle
                handle,
                # last_use
                last_use,
                cluster_name,
                # status
                status.value,
                # autostop
                cluster_name,
                status_lib.ClusterStatus.STOPPED.value,
                # to_down
                cluster_name,
                status_lib.ClusterStatus.STOPPED.value,
                # metadata
                cluster_name,
                # owner
                cluster_name,
                # cluster_hash
                cluster_hash,
                # storage_mounts_metadata
                cluster_name,
                # cluster_ever_up
                cluster_name,
                int(ready),
            ))

    launched_nodes = getattr(cluster_handle, 'launched_nodes', None)
    launched_resources = getattr(cluster_handle, 'launched_resources', None)
    if engine:
        with engine.connect() as connection:
            user_id = _get_user_id()
            cluster_id, cluster_name = _parse_name_values(cluster_name)
            # Prepare the serialized values
            serialized_requested_resources = pickle.dumps(requested_resources)
            serialized_launched_resources = pickle.dumps(launched_resources)
            serialized_usage_intervals = pickle.dumps(usage_intervals)

            # Construct the insert statement with conflict handling
            stmt = insert(cluster_history).values(
                cluster_hash=cluster_hash,
                name=cluster_name,
                num_nodes=launched_nodes,
                requested_resources=serialized_requested_resources,
                launched_resources=serialized_launched_resources,
                usage_intervals=serialized_usage_intervals,
                user_id=user_id
            )

            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=['cluster_hash'],
                set_={
                    'name': stmt.excluded.name,
                    'num_nodes': stmt.excluded.num_nodes,
                    'requested_resources': stmt.excluded.requested_resources,
                    'launched_resources': stmt.excluded.launched_resources,
                    'usage_intervals': stmt.excluded.usage_intervals
                }
            )

            connection.execute(on_conflict_stmt)
            connection.commit()
    else:
        _DB.cursor.execute(
            'INSERT or REPLACE INTO cluster_history'
            '(cluster_hash, name, num_nodes, requested_resources, '
            'launched_resources, usage_intervals) '
            'VALUES ('
            # hash
            '?, '
            # name
            '?, '
            # requested resources
            '?, '
            # launched resources
            '?, '
            # number of nodes
            '?, '
            # usage intervals
            '?)',
            (
                # hash
                cluster_hash,
                # name
                cluster_name,
                # number of nodes
                launched_nodes,
                # requested resources
                pickle.dumps(requested_resources),
                # launched resources
                pickle.dumps(launched_resources),
                # usage intervals
                pickle.dumps(usage_intervals),
            ))

        _DB.conn.commit()


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def update_last_use(cluster_name: str):
    """Updates the last used command for the cluster."""
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = update(clusters).where(clusters.c.id == cluster_id).values(
            last_use=common_utils.get_pretty_entry_point()
        )
        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()
    else:
        _DB.cursor.execute('UPDATE clusters SET last_use=(?) WHERE name=(?)',
                          (common_utils.get_pretty_entry_point(), cluster_name))
        _DB.conn.commit()


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def remove_cluster(cluster_name: str, terminate: bool) -> None:
    """Removes cluster_name mapping."""
    cluster_hash = _get_hash_for_existing_cluster(cluster_name)
    usage_intervals = _get_cluster_usage_intervals(cluster_hash)

    # usage_intervals is not None and not empty
    if usage_intervals:
        assert cluster_hash is not None, cluster_name
        start_time = usage_intervals.pop()[0]
        end_time = int(time.time())
        usage_intervals.append((start_time, end_time))
        _set_cluster_usage_intervals(cluster_hash, usage_intervals)

    if terminate:
        if engine:
            cluster_id, cluster_name = _parse_name_values(cluster_name)
            stmt = delete(clusters).where(clusters.c.id == cluster_id)
        else:
            _DB.cursor.execute('DELETE FROM clusters WHERE name=(?)',
                              (cluster_name,))
    else:
        handle = get_handle_from_cluster_name(cluster_name)
        if handle is None:
            return
        # Must invalidate IP list to avoid directly trying to ssh into a
        # stopped VM, which leads to timeout.
        if hasattr(handle, 'stable_internal_external_ips'):
            handle.stable_internal_external_ips = None
        if engine:
            stmt = update(clusters).where(clusters.c.id == cluster_id).values(
                handle=pickle.dumps(handle),
                status=status_lib.ClusterStatus.STOPPED.value
            )
        else:
            _DB.cursor.execute(
                'UPDATE clusters SET handle=(?), status=(?) '
                'WHERE name=(?)', (
                    pickle.dumps(handle),
                    status_lib.ClusterStatus.STOPPED.value,
                    cluster_name,
                ))

    if engine:
        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()
    else:
        _DB.conn.commit()


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_handle_from_cluster_name(
        cluster_name: str) -> Optional['backends.ResourceHandle']:
    assert cluster_name is not None, 'cluster_name cannot be None'
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = select(clusters.c.handle).where(clusters.c.id == cluster_id)
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()
        if result:
            handle, = result
            return pickle.loads(handle)
    else:
        rows = _DB.cursor.execute('SELECT handle FROM clusters WHERE name=(?)',
                                  (cluster_name,))
        for (handle,) in rows:
            return pickle.loads(handle)
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_glob_cluster_names(cluster_name: str) -> List[str]:
    assert cluster_name is not None, 'cluster_name cannot be None'
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        pattern = cluster_name.replace('*', '%').replace('?', '_')
        stmt = select(clusters.c.name).where(clusters.c.name.like(pattern))
        with engine.connect() as connection:
            result = connection.execute(stmt)
            rows = result.fetchall()
        return [row[0] for row in rows]
    else:
        rows = _DB.cursor.execute('SELECT name FROM clusters WHERE name GLOB (?)',
                                  (cluster_name,))
        return [row[0] for row in rows]


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def set_cluster_status(cluster_name: str,
                       status: status_lib.ClusterStatus) -> None:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = update(clusters).where(clusters.c.id == cluster_id).values(
            status=status.value
        )
        with engine.connect() as connection:
            result = connection.execute(stmt)
            count = result.rowcount
            connection.commit()
    else:
        _DB.cursor.execute('UPDATE clusters SET status=(?) WHERE name=(?)', (
            status.value,
            cluster_name,
        ))
        count = _DB.cursor.rowcount
        _DB.conn.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def set_cluster_autostop_value(cluster_name: str, idle_minutes: int,
                               to_down: bool) -> None:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = update(clusters).where(clusters.c.id == cluster_id).values(
            autostop=idle_minutes,
            to_down=int(to_down)
        )
        with engine.connect() as connection:
            result = connection.execute(stmt)
            count = result.rowcount
            connection.commit()
    else:
        _DB.cursor.execute(
            'UPDATE clusters SET autostop=(?), to_down=(?) WHERE name=(?)', (
                idle_minutes,
                int(to_down),
                cluster_name,
            ))
        count = _DB.cursor.rowcount
        _DB.conn.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_cluster_launch_time(cluster_name: str) -> Optional[int]:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = select(clusters.c.launched_at).where(clusters.c.id == cluster_id)
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()
        if result:
            launch_time, = result
            if launch_time is None:
                return None
            return int(launch_time)
    else:
        rows = _DB.cursor.execute('SELECT launched_at FROM clusters WHERE name=(?)',
                                  (cluster_name,))
        for (launch_time,) in rows:
            if launch_time is None:
                return None
            return int(launch_time)
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_cluster_info(cluster_name: str) -> Optional[Dict[str, Any]]:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = select(clusters.c.metadata).where(clusters.c.id == cluster_id)
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()

        if result:
            metadata, = result
            if metadata is None:
                return None
            return json.loads(metadata)
    else:
        rows = _DB.cursor.execute('SELECT metadata FROM clusters WHERE name=(?)',
                                  (cluster_name,))
        for (metadata,) in rows:
            if metadata is None:
                return None
            return json.loads(metadata)
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def set_cluster_info(cluster_name: str, metadata: Dict[str, Any]) -> None:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = update(clusters).where(clusters.c.id == cluster_id).values(
            metadata=json.dumps(metadata)
        )
        with engine.connect() as connection:
            result = connection.execute(stmt)
            count = result.rowcount
            connection.commit()
    else:
        _DB.cursor.execute('UPDATE clusters SET metadata=(?) WHERE name=(?)', (
            json.dumps(metadata),
            cluster_name,
        ))
        count = _DB.cursor.rowcount
        _DB.conn.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_cluster_storage_mounts_metadata(
        cluster_name: str) -> Optional[Dict[str, Any]]:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = select(clusters.c.storage_mounts_metadata).where(clusters.c.id == cluster_id)
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()
        if result:
            storage_mounts_metadata, = result
            if storage_mounts_metadata is None:
                return None
            return pickle.loads(storage_mounts_metadata)
    else:
        rows = _DB.cursor.execute(
            'SELECT storage_mounts_metadata FROM clusters WHERE name=(?)',
            (cluster_name,))
        for (storage_mounts_metadata,) in rows:
            if storage_mounts_metadata is None:
                return None
            return pickle.loads(storage_mounts_metadata)
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def set_cluster_storage_mounts_metadata(
        cluster_name: str, storage_mounts_metadata: Dict[str, Any]) -> None:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = update(clusters).where(clusters.c.id == cluster_id).values(
            storage_mounts_metadata=pickle.dumps(storage_mounts_metadata)
        )
        with engine.connect() as connection:
            result = connection.execute(stmt)
            count = result.rowcount
            connection.commit()
    else:
        _DB.cursor.execute(
            'UPDATE clusters SET storage_mounts_metadata=(?) WHERE name=(?)', (
                pickle.dumps(storage_mounts_metadata),
                cluster_name,
            ))
        count = _DB.cursor.rowcount
        _DB.conn.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def _get_cluster_usage_intervals(
        cluster_hash: Optional[str]
) -> Optional[List[Tuple[int, Optional[int]]]]:
    if cluster_hash is None:
        return None
    if engine:
        stmt = select(cluster_history.c.usage_intervals).where(cluster_history.c.cluster_hash == cluster_hash)
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()
        if result:
            usage_intervals, = result
            if usage_intervals is None:
                return None
            return pickle.loads(usage_intervals)
    else:
        rows = _DB.cursor.execute(
            'SELECT usage_intervals FROM cluster_history WHERE cluster_hash=(?)',
            (cluster_hash,))
        for (usage_intervals,) in rows:
            if usage_intervals is None:
                return None
            return pickle.loads(usage_intervals)
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def _get_cluster_launch_time(cluster_hash: str) -> Optional[int]:
    usage_intervals = _get_cluster_usage_intervals(cluster_hash)
    if usage_intervals is None:
        return None
    return usage_intervals[0][0]


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def _get_cluster_duration(cluster_hash: str) -> int:
    total_duration = 0
    usage_intervals = _get_cluster_usage_intervals(cluster_hash)

    if usage_intervals is None:
        return total_duration

    for i, (start_time, end_time) in enumerate(usage_intervals):
        # duration from latest start time to time of query
        if start_time is None:
            continue
        if end_time is None:
            assert i == len(usage_intervals) - 1, i
            end_time = int(time.time())
        start_time, end_time = int(start_time), int(end_time)
        total_duration += end_time - start_time
    return total_duration


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def _set_cluster_usage_intervals(
        cluster_hash: str, usage_intervals: List[Tuple[int,
                                                       Optional[int]]]) -> None:
    if engine:
        stmt = update(cluster_history).where(cluster_history.c.cluster_hash == cluster_hash).values(
            usage_intervals=pickle.dumps(usage_intervals)
        )
        with engine.connect() as connection:
            result = connection.execute(stmt)
            count = result.rowcount
            connection.commit()
    else:
        _DB.cursor.execute(
            'UPDATE cluster_history SET usage_intervals=(?) WHERE cluster_hash=(?)',
            (
                pickle.dumps(usage_intervals),
                cluster_hash,
            ))

        count = _DB.cursor.rowcount
        _DB.conn.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster hash {cluster_hash} not found.')


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def set_owner_identity_for_cluster(cluster_name: str,
                                   owner_identity: Optional[List[str]]) -> None:
    if owner_identity is None:
        return
    owner_identity_str = json.dumps(owner_identity)

    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = update(clusters).where(clusters.c.id == cluster_id).values(
            owner=owner_identity_str
        )
        with engine.connect() as connection:
            result = connection.execute(stmt)
            count = result.rowcount
            connection.commit()
    else:
        _DB.cursor.execute('UPDATE clusters SET owner=(?) WHERE name=(?)',
                          (owner_identity_str, cluster_name))

        count = _DB.cursor.rowcount
        _DB.conn.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def _get_hash_for_existing_cluster(cluster_name: str) -> Optional[str]:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = select(clusters.c.cluster_hash).where(clusters.c.id == cluster_id)
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()

        if result:
            cluster_hash, = result
            if cluster_hash is None:
                return None
            return cluster_hash
    else:
        rows = _DB.cursor.execute(
            'SELECT cluster_hash FROM clusters WHERE name=(?)', (cluster_name,))
        for (cluster_hash,) in rows:
            if cluster_hash is None:
                return None
            return cluster_hash
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_launched_resources_from_cluster_hash(
        cluster_hash: str) -> Optional[Tuple[int, Any]]:
    if engine:
        stmt = select(cluster_history.c.num_nodes, cluster_history.c.launched_resources).where(cluster_history.c.cluster_hash == cluster_hash)
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()
        
        if result:
            num_nodes, launched_resources = result
            if num_nodes is None or launched_resources is None:
                return None
            launched_resources = pickle.loads(launched_resources)
            return num_nodes, launched_resources
    else:
        rows = _DB.cursor.execute(
            'SELECT num_nodes, launched_resources '
            'FROM cluster_history WHERE cluster_hash=(?)', (cluster_hash,))
        for (num_nodes, launched_resources) in rows:
            if num_nodes is None or launched_resources is None:
                return None
            launched_resources = pickle.loads(launched_resources)
            return num_nodes, launched_resources
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def _load_owner(record_owner: Optional[str]) -> Optional[List[str]]:
    if record_owner is None:
        return None
    try:
        result = json.loads(record_owner)
        if result is not None and not isinstance(result, list):
            # Backwards compatibility for old records, which were stored as
            # a string instead of a list. It is possible that json.loads
            # will parse the string with all numbers as an int or escape
            # some characters, such as \n, so we need to use the original
            # record_owner.
            return [record_owner]
        return result
    except json.JSONDecodeError:
        # Backwards compatibility for old records, which were stored as
        # a string instead of a list. This will happen when the previous
        # UserId is a string instead of an int.
        return [record_owner]


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def _load_storage_mounts_metadata(
    record_storage_mounts_metadata: Optional[bytes]
) -> Optional[Dict[str, 'Storage.StorageMetadata']]:
    if not record_storage_mounts_metadata:
        return None
    return pickle.loads(record_storage_mounts_metadata)


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_cluster_from_name(
        cluster_name: Optional[str]) -> Optional[Dict[str, Any]]:
    if engine:
        cluster_id, cluster_name = _parse_name_values(cluster_name)
        stmt = select(clusters).where(clusters.c.id == cluster_id)
        with engine.connect() as connection:
            rows = connection.execute(stmt).fetchall()
    else:
        rows = _DB.cursor.execute('SELECT * FROM clusters WHERE name=(?)',
                                  (cluster_name,)).fetchall()
    for row in rows:
        # Explicitly specify the number of fields to unpack, so that
        # we can add new fields to the database in the future without
        # breaking the previous code.
        (id, name, launched_at, handle, last_use, status, autostop, metadata,
         to_down, owner, cluster_hash, storage_mounts_metadata,
         cluster_ever_up) = row[:13]
        # TODO: use namedtuple instead of dict
        record = {
            'name': name,
            'launched_at': launched_at,
            'handle': pickle.loads(handle),
            'last_use': last_use,
            'status': status_lib.ClusterStatus[status],
            'autostop': autostop,
            'to_down': bool(to_down),
            'owner': _load_owner(owner),
            'metadata': json.loads(metadata),
            'cluster_hash': cluster_hash,
            'storage_mounts_metadata':
                _load_storage_mounts_metadata(storage_mounts_metadata),
            'cluster_ever_up': bool(cluster_ever_up),
        }
        return record
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_clusters() -> List[Dict[str, Any]]:
    if engine:
        stmt = select(clusters).order_by(clusters.c.launched_at.desc())
        with engine.connect() as connection:
            rows = connection.execute(stmt).fetchall()
    else:
        rows = _DB.cursor.execute(
            'select * from clusters order by launched_at desc').fetchall()

    records = []
    for row in rows:
        (cluster_id, name, launched_at, handle, last_use, status, autostop, metadata,
         to_down, owner, cluster_hash, storage_mounts_metadata,
         cluster_ever_up) = row[:13]
        # TODO: use namedtuple instead of dict
        # HACK: if the name starts with 'ks-', return cluster_id since serve looks for that.
        record_name = name
        if name.startswith('ks-'):
            record_name = cluster_id
        record = {
            'name': record_name,
            'launched_at': launched_at,
            'handle': pickle.loads(handle),
            'last_use': last_use,
            'status': status_lib.ClusterStatus[status],
            'autostop': autostop,
            'to_down': bool(to_down),
            'owner': _load_owner(owner),
            'metadata': json.loads(metadata),
            'cluster_hash': cluster_hash,
            'storage_mounts_metadata':
                _load_storage_mounts_metadata(storage_mounts_metadata),
            'cluster_ever_up': bool(cluster_ever_up),
        }

        records.append(record)
    return records


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_clusters_from_history() -> List[Dict[str, Any]]:
    if engine:
        ch = cluster_history.alias('ch')
        c = clusters.alias('clusters')
        stmt = select(
            ch.c.cluster_hash,
            ch.c.name,
            ch.c.num_nodes,
            ch.c.launched_resources,
            ch.c.usage_intervals,
            c.c.status
        ).select_from(
            outerjoin(ch, c, ch.c.cluster_hash == c.c.cluster_hash)
        )
        with engine.connect() as connection:
            rows = connection.execute(stmt).fetchall()
    else:
        rows = _DB.cursor.execute(
            'SELECT ch.cluster_hash, ch.name, ch.num_nodes, '
            'ch.launched_resources, ch.usage_intervals, clusters.status  '
            'FROM cluster_history ch '
            'LEFT OUTER JOIN clusters '
            'ON ch.cluster_hash=clusters.cluster_hash ').fetchall()

    # '(cluster_hash, name, num_nodes, requested_resources, '
    #         'launched_resources, usage_intervals) '
    records = []
    for row in rows:
        # TODO: use namedtuple instead of dict

        (
            cluster_hash,
            name,
            num_nodes,
            launched_resources,
            usage_intervals,
            status,
        ) = row[:6]

        if status is not None:
            status = status_lib.ClusterStatus[status]

        record = {
            'name': name,
            'launched_at': _get_cluster_launch_time(cluster_hash),
            'duration': _get_cluster_duration(cluster_hash),
            'num_nodes': num_nodes,
            'resources': pickle.loads(launched_resources),
            'cluster_hash': cluster_hash,
            'usage_intervals': pickle.loads(usage_intervals),
            'status': status,
        }

        records.append(record)

    # sort by launch time, descending in recency
    records = sorted(records, key=lambda record: -record['launched_at'])
    return records


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_cluster_names_start_with(starts_with: str) -> List[str]:
    if engine:
        stmt = select(clusters.c.name).where(clusters.c.name.like(f'{starts_with}%'))
        with engine.connect() as connection:
            rows = connection.execute(stmt).fetchall()
    else:
        rows = _DB.cursor.execute('SELECT name FROM clusters WHERE name LIKE (?)',
                                  (f'{starts_with}%',))
    return [row[0] for row in rows]


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_cached_enabled_clouds() -> List[clouds.Cloud]:
    def get_env_vars_with_prefix(prefix):
        return {key: value for key, value in os.environ.items() if key.startswith(prefix)}

    prefix = 'KOMODO_CLOUD_ENABLED_'
    enabled_cloud_env_vars = get_env_vars_with_prefix(prefix)
    enabled_clouds = []
    for cloud_name, cloud_enabled in enabled_cloud_env_vars.items():
        if cloud_enabled.lower() == '1':
            cloud = clouds.CLOUD_REGISTRY.from_str(cloud_name[len(prefix):])
            if cloud is not None:
                enabled_clouds.append(cloud)
    return enabled_clouds
    # if engine:
    #     cluster_id, cluster_name = _parse_name_values()
    #     stmt = select(config.c.value).where(and_(config.c.cluster_id == cluster_id, config.c.key == _ENABLED_CLOUDS_KEY))
    #     with engine.connect() as connection:
    #         rows = connection.execute(stmt).fetchall()
    #     if not rows:
    #         return []
    #     value, = rows[0]
    #     ret = json.loads(value)
    # else:
    #     rows = _DB.cursor.execute('SELECT value FROM config WHERE key = ?',
    #                               (_ENABLED_CLOUDS_KEY,))
    #     ret = []
    #     for (value,) in rows:
    #         ret = json.loads(value)
    #         break
    # enabled_clouds: List[clouds.Cloud] = []
    # for c in ret:
    #     try:
    #         cloud = clouds.CLOUD_REGISTRY.from_str(c)
    #     except ValueError:
    #         # Handle the case for the clouds whose support has been removed from
    #         # SkyPilot, e.g., 'local' was a cloud in the past and may be stored
    #         # in the database for users before #3037. We should ignore removed
    #         # clouds and continue.
    #         continue
    #     if cloud is not None:
    #         enabled_clouds.append(cloud)
    # return enabled_clouds


def set_enabled_clouds(enabled_clouds: List[str]) -> None:
    pass
    # if engine:
    #     cluster_id, cluster_name = _parse_name_values()
    #     stmt = insert(config).values(
    #         cluster_id=cluster_id,
    #         key=_ENABLED_CLOUDS_KEY,
    #         value=json.dumps(enabled_clouds)
    #     ).on_conflict_do_update(
    #         index_elements=['cluster_id'],
    #         set_={'value': json.dumps(enabled_clouds)}
    #     )
        
    #     with engine.connect() as connection:
    #         connection.execute(stmt)
    #         connection.commit()
    # else:
    #     _DB.cursor.execute('INSERT OR REPLACE INTO config VALUES (?, ?)',
    #                       (_ENABLED_CLOUDS_KEY, json.dumps(enabled_clouds)))
    #     _DB.conn.commit()


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def add_or_update_storage(storage_name: str,
                          storage_handle: 'Storage.StorageMetadata',
                          storage_status: status_lib.StorageStatus):
    storage_launched_at = int(time.time())
    handle = pickle.dumps(storage_handle)
    last_use = common_utils.get_pretty_entry_point()

    def status_check(status):
        return status in status_lib.StorageStatus

    if not status_check(storage_status):
        raise ValueError(f'Error in updating global state. Storage Status '
                         f'{storage_status} is passed in incorrectly')
    
    if engine:
        cluster_id, cluster_name = _parse_name_values()
        stmt = insert(storage).values(
            cluster_id=cluster_id,
            name=storage_name,
            launched_at=storage_launched_at,
            handle=handle,
            last_use=last_use,
            status=storage_status.value
        ).on_conflict_do_update(
            index_elements=['name'],
            set_={
                'launched_at': storage_launched_at,
                'handle': handle,
                'last_use': last_use,
                'status': storage_status.value
            }
        )
        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()
    else:
        _DB.cursor.execute('INSERT OR REPLACE INTO storage VALUES (?, ?, ?, ?, ?)',
                          (storage_name, storage_launched_at, handle, last_use,
                            storage_status.value))
        _DB.conn.commit()


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def remove_storage(storage_name: str):
    """Removes Storage from Database"""
    if engine:
        cluster_id, cluster_name = _parse_name_values()
        stmt = delete(storage).where(and_(storage.c.cluster_id == cluster_id, storage.c.name == storage_name))
        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()
    else:
        _DB.cursor.execute('DELETE FROM storage WHERE name=(?)', (storage_name,))
        _DB.conn.commit()


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def set_storage_status(storage_name: str,
                       status: status_lib.StorageStatus) -> None:
    if engine:
        cluster_id, cluster_name = _parse_name_values()
        stmt = update(storage).where(and_(storage.c.cluster_id == cluster_id, storage.c.name == storage_name)).values(
            status=status.value
        )
        with engine.connect() as connection:
            result = connection.execute(stmt)
            count = result.rowcount
            connection.commit()
    else:
        _DB.cursor.execute('UPDATE storage SET status=(?) WHERE name=(?)', (
            status.value,
            storage_name,
        ))
        count = _DB.cursor.rowcount
        _DB.conn.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Storage {storage_name} not found.')


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_storage_status(storage_name: str) -> Optional[status_lib.StorageStatus]:
    assert storage_name is not None, 'storage_name cannot be None'
    if engine:
        cluster_id, cluster_name = _parse_name_values()
        stmt = select(storage.c.status).where(and_(storage.c.cluster_id == cluster_id, storage.c.name == storage_name))
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()
        if result:
            status, = result
            return status_lib.StorageStatus[status]
    else:
        rows = _DB.cursor.execute('SELECT status FROM storage WHERE name=(?)',
                                  (storage_name,))
        for (status,) in rows:
            return status_lib.StorageStatus[status]
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def set_storage_handle(storage_name: str,
                       handle: 'Storage.StorageMetadata') -> None:
    if engine:
        cluster_id, cluster_name = _parse_name_values()
        stmt = update(storage).where(and_(storage.c.cluster_id == cluster_id, storage.c.name == storage_name)).values(
            handle=pickle.dumps(handle)
        )
        with engine.connect() as connection:
            result = connection.execute(stmt)
            count = result.rowcount
            connection.commit()
    else:
        _DB.cursor.execute('UPDATE storage SET handle=(?) WHERE name=(?)', (
            pickle.dumps(handle),
            storage_name,
        ))
        count = _DB.cursor.rowcount
        _DB.conn.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Storage{storage_name} not found.')


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_handle_from_storage_name(
        storage_name: Optional[str]) -> Optional['Storage.StorageMetadata']:
    if storage_name is None:
        return None
    
    if engine:
        cluster_id, cluster_name = _parse_name_values()
        stmt = select(storage.c.handle).where(and_(storage.c.cluster_id == cluster_id, storage.c.name == storage_name))
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchone()
        if result:
            handle, = result
            if handle is None:
                return None
            return pickle.loads(handle)
    else:
        rows = _DB.cursor.execute('SELECT handle FROM storage WHERE name=(?)',
                                  (storage_name,))
        for (handle,) in rows:
            if handle is None:
                return None
            return pickle.loads(handle)
    return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_glob_storage_name(storage_name: str) -> List[str]:
    assert storage_name is not None, 'storage_name cannot be None'
    if engine:
        pattern = storage_name.replace('*', '%').replace('?', '_')
        cluster_id, cluster_name = _parse_name_values()
        stmt = select(storage.c.name).where(and_(storage.c.cluster_id == cluster_id, storage.c.name.like(pattern)))
        with engine.connect() as connection:
            rows = connection.execute(stmt).fetchall()
    else:
        rows = _DB.cursor.execute('SELECT name FROM storage WHERE name GLOB (?)',
                                  (storage_name,))
    return [row[0] for row in rows]


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_storage_names_start_with(starts_with: str) -> List[str]:
    if engine:
        cluster_id, cluster_name = _parse_name_values()
        stmt = select(storage.c.name).where(and_(storage.c.cluster_id == cluster_id, storage.c.name.like(f'{starts_with}%')))
        with engine.connect() as connection:
            rows = connection.execute(stmt).fetchall()
    else:
        rows = _DB.cursor.execute('SELECT name FROM storage WHERE name LIKE (?)',
                                  (f'{starts_with}%',))
    return [row[0] for row in rows]


@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
def get_storage() -> List[Dict[str, Any]]:
    if engine:
        cluster_id, cluster_name = _parse_name_values()
        stmt = select(storage).where(storage.c.cluster_id == cluster_id)
        with engine.connect() as connection:
            rows = connection.execute(stmt).fetchall()
    else:
        rows = _DB.cursor.execute('select * from storage')
    records = []
    for name, launched_at, handle, last_use, status in rows:
        # TODO: use namedtuple instead of dict
        records.append({
            'name': name,
            'launched_at': launched_at,
            'handle': pickle.loads(handle),
            'last_use': last_use,
            'status': status_lib.StorageStatus[status],
        })
    return records
