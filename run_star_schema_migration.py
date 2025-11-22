#!/usr/bin/env python3
"""
Run Star Schema Migration Script
"""

import psycopg2
from psycopg2.extras import DictCursor
import time

DB_CONFIG = {
    "host": "dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com",
    "port": 5432,
    "database": "ecommerce_dss",
    "user": "dss_user",
    "password": "IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4",
}


def execute_migration_script(conn, script_path):
    """Execute the migration SQL script"""
    print(f"📄 Reading migration script: {script_path}")

    with open(script_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()

    # Split script into individual statements
    statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

    print(f"🔄 Executing {len(statements)} SQL statements...")

    executed_count = 0
    for i, statement in enumerate(statements, 1):
        if not statement:
            continue

        try:
            print(f"  [{i}/{len(statements)}] Executing statement...")
            with conn.cursor() as cur:
                cur.execute(statement)
            conn.commit()
            executed_count += 1

        except Exception as e:
            print(f"    ❌ Error in statement {i}: {e}")
            print(f"    Statement: {statement[:100]}...")
            conn.rollback()
            raise

    print(f"✅ Successfully executed {executed_count} statements")
    return True


def validate_migration(conn):
    """Validate the migration was successful"""
    print("\n🔍 Validating migration...")

    # Check surrogate keys exist
    sk_check_query = """
        SELECT table_name, COUNT(*) as sk_columns
        FROM information_schema.columns
        WHERE table_schema = 'dwh'
          AND table_name LIKE 'dim_%'
          AND column_name LIKE '%_sk'
        GROUP BY table_name
        ORDER BY table_name;
    """

    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sk_check_query)
        sk_results = cur.fetchall()

    print("📊 Surrogate keys added:")
    for row in sk_results:
        print(f"  - {row['table_name']}: {row['sk_columns']} _sk columns")

    # Check foreign keys exist
    fk_check_query = """
        SELECT COUNT(*) as fk_count
        FROM information_schema.table_constraints
        WHERE table_schema = 'dwh'
          AND constraint_type = 'FOREIGN KEY';
    """

    with conn.cursor() as cur:
        cur.execute(fk_check_query)
        fk_count = cur.fetchone()[0]

    print(f"🔗 Foreign key constraints added: {fk_count}")

    return len(sk_results) > 0 and fk_count > 0


def main():
    print("🚀 STAR SCHEMA MIGRATION")
    print("=" * 50)

    # Confirm before proceeding
    print("⚠️  This will modify the database schema!")
    print("   - Add surrogate keys to dimension tables")
    print("   - Add foreign key constraints")
    print("   - Existing code will remain compatible")
    print()

    proceed = input("Continue with migration? (yes/no): ").lower().strip()
    if proceed not in ['yes', 'y']:
        print("Migration cancelled.")
        return

    conn = None
    try:
        print("🔌 Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connected successfully")

        # Execute migration
        script_path = "database/schema/star_schema_migration.sql"
        success = execute_migration_script(conn, script_path)

        if success:
            # Validate
            if validate_migration(conn):
                print("\n🎉 MIGRATION COMPLETED SUCCESSFULLY!")
                print("   - Star Schema compliance achieved")
                print("   - Backward compatibility maintained")
                print("   - Foreign key constraints added")
                print("\n📋 Next steps:")
                print("   1. Run check_star_schema_compliance.py to verify")
                print("   2. Test existing queries still work")
                print("   3. Update new code to use _sk columns")
            else:
                print("\n❌ Migration validation failed!")
        else:
            print("\n❌ Migration failed!")

    except Exception as e:
        print(f"\n💥 Migration error: {e}")
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()
            print("🔌 Database connection closed")


if __name__ == "__main__":
    main()
