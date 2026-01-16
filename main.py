import os
from dotenv import load_dotenv
from sqlalchemy import text
from db.session import db_session


def main():
    load_dotenv()
    test_query = os.getenv("TEST_QUERY")

    if not test_query:
        print("Error: TEST_QUERY not found in .env")
        return

    print("Testing database connection...")
    try:
        # Use the db_session generator
        session_gen = db_session()
        session = next(session_gen)

        # Execute the query from environment variables
        result = session.execute(text(test_query)).all()
        print(f"Database connection successful! Result: {result}")

    except StopIteration:
        print("Error: db_session yielded no session")
    except Exception as e:
        print(f"Database connection failed: {e}")


if __name__ == "__main__":
    main()
