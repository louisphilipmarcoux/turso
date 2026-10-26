//! How explicit transactions behave when a statement inside them fails,
//! checked against SQLite via rusqlite.

use std::sync::Arc;

use crate::common::{limbo_exec_rows, sqlite_exec_rows};
use turso_core::{ConnectOpts, Database, SqliteDialect, IO};

fn memory_connection() -> Arc<turso_core::Connection> {
    let io = Arc::new(turso_core::MemoryIO::new()) as Arc<dyn IO>;
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    db.connect(ConnectOpts::new()).unwrap()
}

#[test]
fn explicit_transaction_survives_runtime_error() {
    // Run the same sequence against `execute` (returns true on success)
    // and `is_autocommit`. The critical assertion is that after a runtime
    // error the connection is still inside the explicit transaction.
    fn check(execute: impl Fn(&str) -> bool, is_autocommit: impl Fn() -> bool) {
        assert!(execute("BEGIN"));
        assert!(!is_autocommit());
        assert!(!execute("SELECT abs(-9223372036854775808)"));
        assert!(
            !is_autocommit(),
            "transaction must stay alive after a runtime error"
        );
        assert!(execute("ROLLBACK"));
        assert!(is_autocommit());
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        || sqlite.is_autocommit(),
    );

    let conn = memory_connection();
    check(|sql| conn.execute(sql).is_ok(), || conn.get_auto_commit());
}

#[test]
fn ordinary_error_in_a_writer_keeps_the_transaction() {
    // These statements get no statement savepoint, but an ordinary runtime
    // error is raised before they write anything, so only the statement is
    // lost and the transaction survives — SQLite behaves the same way.
    for failing in [
        "INSERT INTO t VALUES (abs(-9223372036854775808))",
        "UPDATE t SET x = abs(-9223372036854775808)",
    ] {
        fn check(
            failing: &str,
            execute: impl Fn(&str) -> bool,
            count: impl Fn() -> i64,
            is_autocommit: impl Fn() -> bool,
        ) {
            assert!(execute("CREATE TABLE t (x)"));
            assert!(execute("BEGIN"));
            assert!(execute("INSERT INTO t VALUES (1)"));
            assert!(!execute(failing), "{failing}");
            assert!(
                !is_autocommit(),
                "transaction must survive an ordinary error in a writer: {failing}"
            );
            assert!(execute("COMMIT"), "{failing}");
            assert_eq!(count(), 1, "{failing}");
        }

        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        check(
            failing,
            |sql| sqlite.execute_batch(sql).is_ok(),
            || rows_as_i64(&sqlite_exec_rows(&sqlite, "SELECT count(*) FROM t")),
            || sqlite.is_autocommit(),
        );

        let conn = memory_connection();
        check(
            failing,
            |sql| conn.execute(sql).is_ok(),
            || rows_as_i64(&limbo_exec_rows(&conn, "SELECT count(*) FROM t")),
            || conn.get_auto_commit(),
        );
    }
}

#[test]
fn error_in_a_returning_expression_undoes_the_statement() {
    // RETURNING expressions evaluate after the row is written, so these
    // statements need a statement savepoint: on error the write is undone
    // and the transaction survives — SQLite behaves the same way (it
    // implements RETURNING as an internal trigger).
    for failing in [
        "INSERT INTO t VALUES (2) RETURNING abs(-9223372036854775808)",
        "UPDATE t SET x = x + 10 WHERE x = 1 RETURNING abs(-9223372036854775808)",
        "DELETE FROM t WHERE x = 1 RETURNING abs(-9223372036854775808)",
    ] {
        fn check(
            failing: &str,
            execute: impl Fn(&str) -> bool,
            count: impl Fn(&str) -> i64,
            is_autocommit: impl Fn() -> bool,
        ) {
            assert!(execute("CREATE TABLE t (x)"));
            assert!(execute("BEGIN"));
            assert!(execute("INSERT INTO t VALUES (1)"));
            assert!(!execute(failing), "{failing}");
            assert!(
                !is_autocommit(),
                "transaction must survive a RETURNING expression error: {failing}"
            );
            assert!(execute("COMMIT"), "{failing}");
            assert_eq!(count("SELECT count(*) FROM t"), 1, "{failing}");
            assert_eq!(
                count("SELECT sum(x) FROM t"),
                1,
                "the failed statement's write must be undone: {failing}"
            );
        }

        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        check(
            failing,
            |sql| sqlite.execute_batch(sql).is_ok(),
            |sql| rows_as_i64(&sqlite_exec_rows(&sqlite, sql)),
            || sqlite.is_autocommit(),
        );

        let conn = memory_connection();
        check(
            failing,
            |sql| conn.execute(sql).is_ok(),
            |sql| rows_as_i64(&limbo_exec_rows(&conn, sql)),
            || conn.get_auto_commit(),
        );
    }
}

#[test]
fn out_of_space_without_statement_savepoint_rolls_back_the_transaction() {
    // Running out of space strikes mid-write. A single-row INSERT has no
    // statement savepoint to undo the partial write, so the whole
    // transaction must roll back — SQLite behaves the same way.
    fn check(execute: impl Fn(&str) -> bool, count: impl Fn(&str) -> i64) {
        assert!(execute("PRAGMA max_page_count=10"));
        assert!(execute("CREATE TABLE a (x)"));
        assert!(execute("CREATE TABLE b (x)"));
        assert!(execute("BEGIN"));
        assert!(execute("INSERT INTO a VALUES (1)"));
        assert!(!execute("INSERT INTO b VALUES (randomblob(100000))"));
        assert!(
            !execute("COMMIT"),
            "the transaction must already be rolled back"
        );
        assert_eq!(count("SELECT count(*) FROM a"), 0);
        assert_eq!(count("SELECT count(*) FROM b"), 0);
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        |sql| rows_as_i64(&sqlite_exec_rows(&sqlite, sql)),
    );

    let conn = memory_connection();
    check(
        |sql| conn.execute(sql).is_ok(),
        |sql| rows_as_i64(&limbo_exec_rows(&conn, sql)),
    );
}

#[test]
fn out_of_space_with_statement_savepoint_keeps_the_transaction() {
    // INSERT ... SELECT gets a statement savepoint, so running out of space
    // undoes only the statement and the transaction survives — SQLite
    // behaves the same way.
    fn check(execute: impl Fn(&str) -> bool, count: impl Fn(&str) -> i64) {
        assert!(execute("PRAGMA max_page_count=12"));
        assert!(execute("CREATE TABLE a (x)"));
        assert!(execute("CREATE TABLE src (x)"));
        assert!(execute(
            "INSERT INTO src VALUES (randomblob(2000)), (randomblob(2000)), (randomblob(2000))"
        ));
        assert!(execute("BEGIN"));
        assert!(execute("INSERT INTO a VALUES (1)"));
        assert!(!execute(
            "INSERT INTO a SELECT randomblob(10000) FROM src, src, src"
        ));
        assert!(execute("COMMIT"), "the transaction must still be open");
        assert_eq!(count("SELECT count(*) FROM a"), 1);
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        |sql| rows_as_i64(&sqlite_exec_rows(&sqlite, sql)),
    );

    let conn = memory_connection();
    check(
        |sql| conn.execute(sql).is_ok(),
        |sql| rows_as_i64(&limbo_exec_rows(&conn, sql)),
    );
}

#[test]
fn explicit_transaction_survives_failed_multi_row_write() {
    // A multi-row write runs under a statement savepoint, so when it fails
    // mid-statement only that statement is undone: earlier writes stay and
    // the transaction can still commit.
    fn check(
        execute: impl Fn(&str) -> bool,
        count: impl Fn() -> i64,
        is_autocommit: impl Fn() -> bool,
    ) {
        assert!(execute("CREATE TABLE t (x INTEGER UNIQUE)"));
        assert!(execute("BEGIN"));
        assert!(execute("INSERT INTO t VALUES (1), (2), (3)"));
        assert!(!execute("INSERT INTO t SELECT x + 2 FROM t"));
        assert!(
            !is_autocommit(),
            "transaction must stay alive after a failed multi-row write"
        );
        assert_eq!(
            count(),
            3,
            "the failed statement must be fully undone and earlier writes kept"
        );
        assert!(execute("COMMIT"));
        assert_eq!(count(), 3);
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        || rows_as_i64(&sqlite_exec_rows(&sqlite, "SELECT count(*) FROM t")),
        || sqlite.is_autocommit(),
    );

    let conn = memory_connection();
    check(
        |sql| conn.execute(sql).is_ok(),
        || rows_as_i64(&limbo_exec_rows(&conn, "SELECT count(*) FROM t")),
        || conn.get_auto_commit(),
    );
}

fn rows_as_i64(rows: &[Vec<rusqlite::types::Value>]) -> i64 {
    match &rows[0][0] {
        rusqlite::types::Value::Integer(n) => *n,
        other => panic!("expected an integer, got {other:?}"),
    }
}
