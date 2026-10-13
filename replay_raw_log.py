"""
Copies requests from data/raw-requests.jsonl into the database when they are
missing there, for example after the database was down for a while.

It is safe to run more than once: a request whose raw_id is already in the
`requests` table is skipped. Rows added this way have source = 'replay'.

Run it next to app.py, with the same .env file:

    python replay_raw_log.py
"""
import json
import os

import app as server


def main():
    if not os.path.exists(server.RAW_LOG_PATH):
        print("Nothing to replay: " + server.RAW_LOG_PATH + " does not exist.")
        return

    connection = server.get_connection()
    server.ensure_tables(connection)
    with connection.cursor() as cursor:
        cursor.execute("SELECT `raw_id` FROM `requests`")
        already_stored = set(row[0] for row in cursor.fetchall())
    connection.rollback()

    added = 0
    skipped = 0
    failed = 0
    unreadable = 0
    with open(server.RAW_LOG_PATH, encoding="utf-8") as file:
        line_number = 0
        for line in file:
            line_number += 1
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                raw_id = record["raw_id"]
            except (ValueError, KeyError, TypeError):
                # e.g. the last line, if the server stopped halfway through writing it
                print("Line " + str(line_number) + " is not a complete record, skipped.")
                unreadable += 1
                continue
            if raw_id in already_stored:
                skipped += 1
                continue
            outcome = server.understand_request(record)
            if server.save_to_database(record, outcome, "replay"):
                already_stored.add(raw_id)
                added += 1
            else:
                failed += 1

    print("Added " + str(added) + ", already there " + str(skipped)
          + ", failed " + str(failed) + ", unreadable lines " + str(unreadable) + ".")


if __name__ == "__main__":
    main()
